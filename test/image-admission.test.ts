import { test, type TestContext } from "node:test";
import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as net from "node:net";
import * as os from "node:os";
import * as path from "node:path";
import { createHash, randomUUID } from "node:crypto";
import {
	ATTACHMENT_CAPABILITY,
	initializeLiveAttachmentConsumer,
	initializeLiveTransferMarker,
	type LiveInputImageReaderDescriptor,
	type PrepareRpcPromptFiles,
	type PrepareRpcPromptFilesInput,
} from "../attachment-transfer.ts";
import {
	detectLiveImageAdmissionCapability,
	startLiveTransport,
	type LiveEvent,
	type LiveHostDispatchOptions,
	type LiveIdentity,
	type LiveImageAdmissionCapability,
	type LiveOutputArtifactPresentationCapability,
	type LiveSnapshot,
	type LiveTransportLimits,
	type LiveTransportHost,
	type LiveTransportServer,
} from "../live-transport.ts";

/** Pi's exact seven-field capability, mirrored for fixtures only. */
const IMAGE_ADMISSION_CAPABILITY: LiveImageAdmissionCapability = {
	protocol: "pi.image-admission",
	version: 1,
	preparation: "rpc-prompt-files-strict-v1",
	dispatch: "standard-user-strict-v1",
	requestValidation: "normalized-messages-v1",
	virtualModels: "stable-profile-v1",
	binaryInLive: false,
};

/** The thirteen admission-phase codes of the native contract. */
const ADMISSION_CODES = [
	"image_command_unsupported",
	"image_hook_unsupported",
	"image_admission_cancelled",
	"image_admission_state_changed",
	"image_input_blocked",
	"image_model_unavailable",
	"image_model_unsupported",
	"image_virtual_profile_required",
	"image_processing_failed",
	"image_size_exceeded",
	"image_message_limit_exceeded",
	"image_request_limit_exceeded",
	"image_native_capacity_exceeded",
] as const;

/** The five request-phase codes, which must never become an admission rejection. */
const REQUEST_CODES = [
	"image_input_blocked",
	"image_model_unsupported",
	"image_virtual_route_incompatible",
	"image_message_limit_exceeded",
	"image_request_limit_exceeded",
] as const;

const PREPARATION_CODES = [
	"file_not_found",
	"file_not_regular",
	"file_read_failed",
	"text_file_too_large",
	"text_total_too_large",
	"image_processing_failed",
	"invalid_declared_media_type",
	"unsupported_image_type",
	"image_source_too_large",
	"image_output_too_large",
	"image_dimensions_invalid",
	"image_dimensions_exceeded",
	"animated_image_unsupported",
	"image_decode_failed",
	"image_decode_timeout",
	"image_decoder_busy",
	"image_decoder_unavailable",
] as const;

const OUTPUT_ARTIFACT_PRESENTATION_CAPABILITY: LiveOutputArtifactPresentationCapability = {
	protocol: "pi.output-artifact-presentation",
	version: 1,
	messageBlock: "output_artifact_reference",
	metadataQuery: "artifact_history",
	coverage: "direct_tool_result_content",
	states: ["resolved", "partial", "missing", "duplicate", "unsupported"],
	nestedToolResults: "unsupported_placeholder",
	binaryInLive: false,
};

const inputImageProjection = () => ({
	inputImageCapability: {
		protocol: "pi.input-images",
		version: 1,
		messageBlock: "input_image_reference",
		metadataQuery: "input_image_history",
		maxImageBytes: 33554432,
		copyChunkBytes: 262144,
		maxPendingImages: 256,
		maxPendingDeclaredBytes: 268435456,
		maxPendingMetadataBytes: 262144,
		binaryInLive: false,
	},
	inputImages: [],
	inputImageCoverage: {
		scope: "current_branch_input_images",
		leafId: null,
		startIndex: 0,
		endIndexExclusive: 0,
		totalInputImages: 0,
		byteLength: 2,
		hasOlder: false,
		hasNewer: false,
	},
	pendingInputImages: [],
});

/** The private input-image reader requires a canonical native session UUID. */
const SESSION_ID = "6f1d0c3a-57a1-4a2c-9c0e-2b7f5d8e1a44";

type DispatchCall = {
	target: LiveIdentity;
	command: unknown;
	options: LiveHostDispatchOptions | undefined;
	argumentCount: number;
};

class AdmissionHost implements LiveTransportHost {
	readonly identity: LiveIdentity = { streamId: "stream-adm", sessionEpoch: 4, sessionId: SESSION_ID };
	sequence = 0;
	snapshotExtra: Record<string, unknown> = {};
	readonly dispatchCalls: DispatchCall[] = [];
	dispatchImpl: (call: DispatchCall, output: (response: unknown) => void) => Promise<unknown | undefined> =
		async (call, output) => {
			output({ type: (call.command as { type: string }).type, success: true });
			return undefined;
		};
	private readonly listeners = new Set<(event: LiveEvent) => void>();

	snapshot(): LiveSnapshot {
		return { ...this.identity, sequence: this.sequence, ...this.snapshotExtra } as LiveSnapshot;
	}

	subscribe(listener: (event: LiveEvent) => void): () => void {
		this.listeners.add(listener);
		return () => this.listeners.delete(listener);
	}

	dispatch(
		target: LiveIdentity,
		command: unknown,
		output: (response: unknown) => void,
		options?: LiveHostDispatchOptions,
	): Promise<unknown | undefined> {
		const call: DispatchCall = { target, command, options, argumentCount: arguments.length };
		this.dispatchCalls.push(call);
		return this.dispatchImpl(call, output);
	}

	emit(type: string, data: Record<string, unknown> = {}): LiveEvent {
		const event = { ...this.identity, sequence: ++this.sequence, type, ...data } as LiveEvent;
		for (const listener of [...this.listeners]) listener(event);
		return event;
	}

	close(): void {
		this.listeners.clear();
	}
}

function addDirectInputImageMethods(host: AdmissionHost): void {
	Object.assign(host, {
		getCurrentBranchInputImage: () => {
			throw Object.assign(new Error("missing"), { code: "input_image_not_found" });
		},
		copyInputImageBytes: async () => ({ byteLength: 0, sha256: "0".repeat(64) }),
	});
}

class JsonlClient {
	private readonly frames: any[] = [];
	private readonly waiters: Array<() => void> = [];
	private buffer = "";
	readonly socket: net.Socket;

	private constructor(socket: net.Socket) {
		this.socket = socket;
		socket.setEncoding("utf8");
		socket.on("data", (chunk) => {
			this.buffer += chunk;
			const lines = this.buffer.split("\n");
			this.buffer = lines.pop()!;
			for (const line of lines) if (line) this.frames.push(JSON.parse(line));
			for (const wake of this.waiters.splice(0)) wake();
		});
	}

	static async connect(socketPath: string): Promise<JsonlClient> {
		const socket = net.createConnection(socketPath);
		await new Promise<void>((resolve, reject) => {
			socket.once("connect", resolve);
			socket.once("error", reject);
		});
		return new JsonlClient(socket);
	}

	send(value: unknown): void {
		this.socket.write(JSON.stringify(value) + "\n");
	}

	async next(timeoutMs = 2000): Promise<any> {
		const deadline = Date.now() + timeoutMs;
		while (this.frames.length === 0) {
			const remaining = deadline - Date.now();
			if (remaining <= 0) throw new Error("timed out waiting for JSONL frame");
			await new Promise<void>((resolve, reject) => {
				const timer = setTimeout(() => reject(new Error("timed out waiting for JSONL frame")), remaining);
				this.waiters.push(() => {
					clearTimeout(timer);
					resolve();
				});
			});
		}
		return this.frames.shift();
	}

	close(): void {
		this.socket.destroy();
	}
}

type AdmissionFixture = {
	root: string;
	inbox: string;
	socketPath: string;
	inputSocketPath: string;
	apiSocketPath: string;
	threadId: string;
	host: AdmissionHost;
	errors: unknown[];
	preparations: PrepareRpcPromptFilesInput[];
};

async function admissionFixture(t: TestContext): Promise<AdmissionFixture> {
	const root = await fs.promises.mkdtemp(path.join(os.tmpdir(), "pi-adm-"));
	const inbox = path.join(root, "inbox");
	await fs.promises.mkdir(path.join(inbox, "leases"), { recursive: true, mode: 0o700 });
	await fs.promises.chmod(inbox, 0o700);
	await fs.promises.chmod(path.join(inbox, "leases"), 0o700);
	const host = new AdmissionHost();
	host.snapshotExtra = { ...inputImageProjection(), imageAdmissionCapability: { ...IMAGE_ADMISSION_CAPABILITY } };
	addDirectInputImageMethods(host);
	t.after(async () => {
		await fs.promises.rm(root, { recursive: true, force: true });
	});
	return {
		root,
		inbox,
		socketPath: path.join(root, "live.sock"),
		inputSocketPath: path.join(root, "in.sock"),
		apiSocketPath: path.join(root, "sesh.sock"),
		threadId: randomUUID(),
		host,
		errors: [],
		preparations: [],
	};
}

type StartOptions = {
	prepare?: PrepareRpcPromptFiles;
	importedCapability?: unknown;
	inputReader?: boolean;
	managed?: { limits?: Record<string, number> };
	limits?: Partial<LiveTransportLimits>;
	now?: () => number;
};

async function startAdmission(
	t: TestContext,
	fixture: AdmissionFixture,
	options: StartOptions = {},
): Promise<LiveTransportServer> {
	const implementation = options.prepare ?? (async ({ command }) => command);
	const prepareRpcPromptFiles: PrepareRpcPromptFiles = async (input) => {
		fixture.preparations.push(input);
		return await implementation(input);
	};
	const server = await startLiveTransport(fixture.host, {
		socketPath: fixture.socketPath,
		inputImageSocketPath: fixture.inputSocketPath,
		onError: (error) => fixture.errors.push(error),
		...(options.limits ? { limits: options.limits } : {}),
		...(options.now ? { now: options.now } : {}),
		attachments: {
			threadId: fixture.threadId,
			inboxPath: fixture.inbox,
			apiSocketPath: fixture.apiSocketPath,
			prepareRpcPromptFiles,
			importedImageAdmissionCapability: "importedCapability" in options
				? options.importedCapability
				: { ...IMAGE_ADMISSION_CAPABILITY },
		},
		...(options.inputReader === false ? {} : {
			inputImageReader: {
				threadId: fixture.threadId,
				inboxPath: fixture.inbox,
				apiSocketPath: fixture.apiSocketPath,
			},
		}),
		...(options.managed ? {
			managedArtifacts: {
				threadId: fixture.threadId,
				inboxPath: fixture.inbox,
				apiSocketPath: fixture.apiSocketPath,
				...(options.managed.limits ? { limits: options.managed.limits as never } : {}),
				client: {
					async resolveMapping() { throw new Error("no publication expected"); },
					async putMappingContent() { throw new Error("no publication expected"); },
					async failMapping() { throw new Error("no publication expected"); },
				},
			},
		} : {}),
	});
	t.after(async () => {
		await server.close().catch(() => undefined);
	});
	return server;
}

async function writeLease(
	fixture: AdmissionFixture,
	input: { requestId: string; name?: string; bytes?: Buffer; mediaType?: string | null },
): Promise<{ leaseId: string; sha256: string; bytes: Buffer }> {
	const leaseId = randomUUID();
	const bytes = input.bytes ?? Buffer.from("lease bytes");
	const directory = path.join(fixture.inbox, "leases", leaseId);
	await fs.promises.mkdir(directory, { mode: 0o700 });
	await fs.promises.chmod(directory, 0o700);
	const payload = path.join(directory, "payload");
	await fs.promises.writeFile(payload, bytes, { mode: 0o600 });
	await fs.promises.chmod(payload, 0o600);
	const sha256 = createHash("sha256").update(bytes).digest("hex");
	const mediaType = input.mediaType === undefined ? "text/plain" : input.mediaType;
	const lease = {
		protocol: "sesh.pi-transfer",
		version: 1,
		type: "attachment-lease",
		lease_id: leaseId,
		thread_id: fixture.threadId,
		attachment_id: randomUUID(),
		target: {
			stream_id: fixture.host.identity.streamId,
			session_id: fixture.host.identity.sessionId,
			session_epoch: fixture.host.identity.sessionEpoch,
		},
		request_id: input.requestId,
		name: input.name ?? "attachment.txt",
		...(mediaType === null ? {} : { media_type: mediaType }),
		size: bytes.length,
		sha256,
		payload: "payload",
		expires_at_unix_ms: Date.now() + 60_000,
	};
	const metadata = path.join(directory, "lease.json");
	await fs.promises.writeFile(metadata, JSON.stringify(lease), { mode: 0o600 });
	await fs.promises.chmod(metadata, 0o600);
	return { leaseId, sha256, bytes };
}

const attachmentDispatch = (
	fixture: AdmissionFixture,
	requestId: string,
	command: Record<string, unknown>,
	leaseIds: readonly string[],
) => ({
	protocol: "live-v1",
	id: requestId,
	op: "dispatch",
	target: fixture.host.identity,
	command,
	attachments: leaseIds.map((leaseId) => ({ leaseId })),
});

const capabilitiesOf = async (client: JsonlClient, id: string) => {
	client.send({ protocol: "live-v1", id, op: "capabilities" });
	return (await client.next()).result;
};

const getResult = async (
	client: JsonlClient,
	fixture: AdmissionFixture,
	id: string,
	requestId: string,
) => {
	client.send({ protocol: "live-v1", id, op: "get_result", target: fixture.host.identity, requestId });
	return await client.next();
};

const errorText = (errors: readonly unknown[]) =>
	errors.map((error) => (error instanceof Error ? error.message : String(error))).join("\n");

test("the image-admission capability is detected only in its exact seven-field form", () => {
	assert.deepEqual(detectLiveImageAdmissionCapability({ ...IMAGE_ADMISSION_CAPABILITY }), IMAGE_ADMISSION_CAPABILITY);
	assert.deepEqual(
		detectLiveImageAdmissionCapability({
			binaryInLive: false,
			virtualModels: "stable-profile-v1",
			requestValidation: "normalized-messages-v1",
			dispatch: "standard-user-strict-v1",
			preparation: "rpc-prompt-files-strict-v1",
			version: 1,
			protocol: "pi.image-admission",
		}),
		IMAGE_ADMISSION_CAPABILITY,
		"object key order is irrelevant",
	);

	const mutable: Record<string, unknown> = { ...IMAGE_ADMISSION_CAPABILITY };
	const detached = detectLiveImageAdmissionCapability(mutable)!;
	assert.equal(Object.isFrozen(detached), true);
	mutable.protocol = "pi.other";
	mutable.binaryInLive = true;
	assert.deepEqual(detached, IMAGE_ADMISSION_CAPABILITY, "the detector returns a detached value");

	for (const field of Object.keys(IMAGE_ADMISSION_CAPABILITY)) {
		const missing: Record<string, unknown> = { ...IMAGE_ADMISSION_CAPABILITY };
		delete missing[field];
		assert.equal(detectLiveImageAdmissionCapability(missing), undefined, `missing ${field}`);
		assert.equal(
			detectLiveImageAdmissionCapability({ ...IMAGE_ADMISSION_CAPABILITY, [field]: "wrong" }),
			undefined,
			`wrong ${field}`,
		);
	}
	assert.equal(detectLiveImageAdmissionCapability({ ...IMAGE_ADMISSION_CAPABILITY, version: 2 }), undefined);
	assert.equal(detectLiveImageAdmissionCapability({ ...IMAGE_ADMISSION_CAPABILITY, version: "1" }), undefined);
	assert.equal(detectLiveImageAdmissionCapability({ ...IMAGE_ADMISSION_CAPABILITY, binaryInLive: 0 }), undefined);
	assert.equal(detectLiveImageAdmissionCapability({ ...IMAGE_ADMISSION_CAPABILITY, extra: true }), undefined);
	assert.equal(
		detectLiveImageAdmissionCapability({
			...IMAGE_ADMISSION_CAPABILITY,
			classifier: "magic-bytes-v1",
			formats: ["png"],
			decoder: "bounded-worker-v1",
			sourceLimitBytes: 16777216,
			declaredMediaType: "trusted",
			unchangedBytes: true,
			payloadValidation: "serialized-v1",
		}),
		undefined,
		"the obsolete broad capability shape is rejected",
	);
	for (const value of [undefined, null, 1, "pi.image-admission", [{ ...IMAGE_ADMISSION_CAPABILITY }], () => undefined]) {
		assert.equal(detectLiveImageAdmissionCapability(value), undefined);
	}
});

test("the complete same-target stack publishes imageInput and forwards exact strict dispatch", async (t) => {
	const fixture = await admissionFixture(t);
	fixture.host.snapshotExtra = {
		...fixture.host.snapshotExtra,
		outputArtifactPresentationCapability: { ...OUTPUT_ARTIFACT_PRESENTATION_CAPABILITY },
	};
	const pngBase64 = Buffer.from("PNG-CONVERTED-PREPARED-BYTES").toString("base64");
	const secondBase64 = Buffer.from("SECOND-PREPARED-IMAGE").toString("base64");
	const prepared: Record<string, unknown>[] = [];
	await startAdmission(t, fixture, {
		prepare: async ({ command, files }) => {
			const result = files.some((file) => file.displayName.endsWith(".bmp"))
				? {
					...command,
					images: [
						{ type: "image", data: pngBase64, mimeType: "image/png" },
						{ type: "image", data: secondBase64, mimeType: "image/png" },
					],
				}
				: { ...command, message: `<file name="${files[0]!.displayName}">text</file>` };
			prepared.push(result);
			return result;
		},
	});

	const marker = JSON.parse(await fs.promises.readFile(path.join(fixture.inbox, "consumer.json"), "utf8"));
	assert.deepEqual(marker.capabilities, ["attachment-lease-v1", "input-image-read-v1"], errorText(fixture.errors));
	assert.equal(marker.input_image_reader.protocol, "pi.input-image-read");
	assert.match(marker.input_image_reader.listener_generation, /^[0-9a-f-]{36}$/);

	const client = await JsonlClient.connect(fixture.socketPath);
	t.after(() => client.close());
	const capabilities = await capabilitiesOf(client, "ready-caps");
	assert.equal(capabilities.attachments.imageInput, true);
	assert.deepEqual(capabilities.attachments, { ...ATTACHMENT_CAPABILITY, imageInput: true });
	assert.deepEqual(capabilities.resultStates, ["running", "completed", "failed", "unavailable", "uncertain"]);
	assert.ok(capabilities.inputImages, "projection capability stays additive");
	assert.ok(capabilities.inputImageBytes, "the private byte reader stays additive");
	assert.ok(capabilities.outputArtifactPresentation, "output-v2 stays independent");

	// Strict image dispatch: exact prepared object, exact fourth argument.
	const imageRequestId = randomUUID();
	const imageLease = await writeLease(fixture, {
		requestId: imageRequestId,
		name: "diagram.bmp",
		bytes: Buffer.from("BM-RAW-UPLOADED-SOURCE-BYTES"),
		mediaType: "image/BMP; charset=binary",
	});
	client.send(attachmentDispatch(fixture, imageRequestId, {
		type: "prompt",
		id: "native-prompt-7",
		message: "inspect",
		streamingBehavior: "steer",
	}, [imageLease.leaseId]));
	const imageResponse = await client.next();
	assert.equal(imageResponse.ok, true);
	assert.equal(imageResponse.result.status, "completed");

	assert.equal(fixture.preparations.length, 1);
	const preparation = fixture.preparations[0]!;
	assert.equal(preparation.imageAdmission, "strict-v1");
	assert.equal(preparation.files.length, 1);
	assert.equal(preparation.files[0]!.displayName, "diagram.bmp");
	assert.equal(preparation.files[0]!.declaredMediaType, "image/BMP; charset=binary");
	assert.equal(preparation.maxTextBytesPerFile, ATTACHMENT_CAPABILITY.maxTextBytesPerFile);
	assert.equal(preparation.maxTextBytesTotal, ATTACHMENT_CAPABILITY.maxTextBytesTotal);

	assert.equal(fixture.host.dispatchCalls.length, 1);
	const imageCall = fixture.host.dispatchCalls[0]!;
	assert.equal(imageCall.argumentCount, 4);
	assert.deepEqual(imageCall.options, { imageAdmission: "strict-v1" });
	assert.equal(imageCall.command, prepared[0], "the exact prepared object reaches dispatch without reconstruction");
	assert.deepEqual(imageCall.command, {
		type: "prompt",
		id: "native-prompt-7",
		message: "inspect",
		streamingBehavior: "steer",
		images: [
			{ type: "image", data: pngBase64, mimeType: "image/png" },
			{ type: "image", data: secondBase64, mimeType: "image/png" },
		],
	});
	assert.deepEqual(imageCall.target, fixture.host.identity, "the original mutation target stays authoritative");

	// Nothing about the image, its raw bytes, its digest or the strict option is on the wire.
	const retainedImage = await getResult(client, fixture, "ready-image-result", imageRequestId);
	assert.equal(retainedImage.result.status, "completed");
	const observable = JSON.stringify([imageResponse, retainedImage, capabilities]);
	for (const secret of [
		pngBase64,
		secondBase64,
		imageLease.bytes.toString("utf8"),
		imageLease.sha256,
		preparation.files[0]!.path,
		"strict-v1",
		"image/BMP",
	]) {
		assert.ok(!observable.includes(secret), `live frames must not contain ${secret.slice(0, 24)}`);
	}

	// Strict preparation of text-only attachments dispatches with no options at all.
	const textRequestId = randomUUID();
	const textLease = await writeLease(fixture, { requestId: textRequestId, name: "notes.txt" });
	client.send(attachmentDispatch(fixture, textRequestId, { type: "prompt", message: "read" }, [textLease.leaseId]));
	assert.equal((await client.next()).result.status, "completed");
	assert.equal(fixture.preparations[1]!.imageAdmission, "strict-v1");
	const textCall = fixture.host.dispatchCalls[1]!;
	assert.equal(textCall.argumentCount, 3, "text-only strict preparation never receives the fourth argument");
	assert.equal(textCall.options, undefined);

	// steer and follow_up forward the same exact option for ready strict images.
	for (const type of ["steer", "follow_up"] as const) {
		const requestId = randomUUID();
		const lease = await writeLease(fixture, { requestId, name: `${type}.bmp`, mediaType: "image/bmp" });
		client.send(attachmentDispatch(fixture, requestId, { type, id: `native-${type}`, message: type }, [lease.leaseId]));
		assert.equal((await client.next()).result.status, "completed", type);
		const call = fixture.host.dispatchCalls.at(-1)!;
		assert.equal(call.argumentCount, 4, type);
		assert.deepEqual(call.options, { imageAdmission: "strict-v1" }, type);
		assert.equal((call.command as { type: string }).type, type);
		assert.equal((call.command as { id: string }).id, `native-${type}`);
	}
	assert.deepEqual(fixture.errors, []);
});

test("strict preparation runs on a matched runtime while incomplete readiness refuses images", async (t) => {
	const fixture = await admissionFixture(t);
	fixture.host.snapshotExtra = {
		...fixture.host.snapshotExtra,
		outputArtifactPresentationCapability: { ...OUTPUT_ARTIFACT_PRESENTATION_CAPABILITY },
	};
	const imageBase64 = Buffer.from("UNREADY-PREPARED-IMAGE").toString("base64");
	await startAdmission(t, fixture, {
		inputReader: false,
		prepare: async ({ command, files }) => files.some((file) => file.displayName.endsWith(".png"))
			? { ...command, images: [{ type: "image", data: imageBase64, mimeType: "image/png" }] }
			: command,
	});

	const client = await JsonlClient.connect(fixture.socketPath);
	t.after(() => client.close());
	const capabilities = await capabilitiesOf(client, "unready-caps");
	assert.equal(capabilities.attachments.imageInput, false, "projection-only runtimes stay false");
	assert.equal(capabilities.inputImageBytes, undefined);
	assert.ok(capabilities.inputImages);
	assert.ok(capabilities.outputArtifactPresentation, "complete output-v2 never compensates for an incomplete input stack");

	const requestId = randomUUID();
	const lease = await writeLease(fixture, { requestId, name: "photo.png", mediaType: "image/png" });
	client.send(attachmentDispatch(fixture, requestId, { type: "prompt", message: "inspect" }, [lease.leaseId]));
	const refused = await client.next();
	assert.equal(refused.ok, false);
	assert.equal(refused.error.code, "image_input_unsupported");
	assert.match(refused.error.message, /no prompt was dispatched/);
	assert.ok(!JSON.stringify(refused).includes(imageBase64));
	assert.equal(fixture.host.dispatchCalls.length, 0, "no prepared image is dispatched while readiness is false");
	// Strict native classification still ran, so unsupported bytes cannot become text.
	assert.equal(fixture.preparations[0]!.imageAdmission, "strict-v1");

	const retained = await getResult(client, fixture, "unready-result", requestId);
	assert.equal(retained.result.status, "failed");
	assert.equal(retained.result.error.code, "image_input_unsupported");
	client.send(attachmentDispatch(fixture, requestId, { type: "prompt", message: "again" }, [lease.leaseId]));
	assert.equal((await client.next()).error.code, "duplicate_request");

	// Text attachments stay fully available on the same runtime.
	const textRequestId = randomUUID();
	const textLease = await writeLease(fixture, { requestId: textRequestId, name: "notes.txt" });
	client.send(attachmentDispatch(fixture, textRequestId, { type: "prompt", message: "read" }, [textLease.leaseId]));
	assert.equal((await client.next()).result.status, "completed");
	assert.equal(fixture.host.dispatchCalls.length, 1);
	assert.equal(fixture.host.dispatchCalls[0]!.argumentCount, 3);
});

test("a snapshot capability alone, a later snapshot, the environment and start options cannot enable images", async (t) => {
	const imageBase64 = Buffer.from("MUST-NOT-BE-ADMITTED").toString("base64");
	const imagePreparer: PrepareRpcPromptFiles = async ({ command, files }) =>
		files.some((file) => file.displayName.endsWith(".png"))
			? { ...command, images: [{ type: "image", data: imageBase64, mimeType: "image/png" }] }
			: command;

	// The exact capability in the snapshot, with the complete reader stack, is not enough:
	// the helper's own module instance must export the same capability.
	const withoutImport = await admissionFixture(t);
	await startAdmission(t, withoutImport, { importedCapability: undefined, prepare: imagePreparer });
	const withoutImportMarker = JSON.parse(await fs.promises.readFile(path.join(withoutImport.inbox, "consumer.json"), "utf8"));
	assert.deepEqual(withoutImportMarker.capabilities, ["attachment-lease-v1", "input-image-read-v1"]);
	const withoutImportClient = await JsonlClient.connect(withoutImport.socketPath);
	t.after(() => withoutImportClient.close());
	const withoutImportCapabilities = await capabilitiesOf(withoutImportClient, "no-import-caps");
	assert.equal(withoutImportCapabilities.attachments.imageInput, false);
	assert.ok(withoutImportCapabilities.inputImageBytes, "the byte reader is unaffected");
	const withoutImportRequestId = randomUUID();
	const withoutImportLease = await writeLease(withoutImport, { requestId: withoutImportRequestId, name: "p.png", mediaType: "image/png" });
	withoutImportClient.send(attachmentDispatch(withoutImport, withoutImportRequestId, { type: "prompt", message: "x" }, [withoutImportLease.leaseId]));
	assert.equal((await withoutImportClient.next()).error.code, "image_input_unsupported");
	assert.equal(withoutImport.preparations[0]!.imageAdmission, undefined, "generic preparation on an unmatched runtime");
	assert.equal(withoutImport.host.dispatchCalls.length, 0);

	// Every malformed, partial, obsolete or wrong snapshot capability leaves support false,
	// and nothing else about the runtime changes.
	const snapshotVariants: Array<[string, unknown]> = [
		["absent", undefined],
		["missing field", (() => { const { binaryInLive, ...rest } = IMAGE_ADMISSION_CAPABILITY; return rest; })()],
		["extra field", { ...IMAGE_ADMISSION_CAPABILITY, imageInput: true }],
		["wrong literal", { ...IMAGE_ADMISSION_CAPABILITY, dispatch: "standard-user-v1" }],
		["obsolete broad", { ...IMAGE_ADMISSION_CAPABILITY, classifier: "magic-bytes-v1", payloadValidation: "serialized-v1" }],
		["not a record", "pi.image-admission"],
	];
	for (const [label, capability] of snapshotVariants) {
		const fixture = await admissionFixture(t);
		fixture.host.snapshotExtra = {
			...inputImageProjection(),
			...(capability === undefined ? {} : { imageAdmissionCapability: capability }),
		};
		await startAdmission(t, fixture, { prepare: imagePreparer });
		const client = await JsonlClient.connect(fixture.socketPath);
		t.after(() => client.close());
		const capabilities = await capabilitiesOf(client, "variant-caps");
		assert.equal(capabilities.attachments.imageInput, false, label);
		assert.ok(capabilities.inputImages, label);
		assert.ok(capabilities.inputImageBytes, label);
	}

	// A malformed imported capability is equally refused against an exact snapshot.
	for (const imported of [
		{ ...IMAGE_ADMISSION_CAPABILITY, version: 2 },
		{ ...IMAGE_ADMISSION_CAPABILITY, extra: 1 },
		{},
		true,
	]) {
		const fixture = await admissionFixture(t);
		await startAdmission(t, fixture, { importedCapability: imported, prepare: imagePreparer });
		const client = await JsonlClient.connect(fixture.socketPath);
		t.after(() => client.close());
		assert.equal((await capabilitiesOf(client, "imported-caps")).attachments.imageInput, false);
	}

	// A snapshot that gains the capability after startup never changes this startup's state.
	const later = await admissionFixture(t);
	later.host.snapshotExtra = inputImageProjection();
	await startAdmission(t, later, { prepare: imagePreparer });
	const laterClient = await JsonlClient.connect(later.socketPath);
	t.after(() => laterClient.close());
	assert.equal((await capabilitiesOf(laterClient, "later-before")).attachments.imageInput, false);
	later.host.snapshotExtra = {
		...inputImageProjection(),
		imageAdmissionCapability: { ...IMAGE_ADMISSION_CAPABILITY },
	};
	later.host.emit("session_event", { origin: "interactive" });
	laterClient.send({ protocol: "live-v1", id: "later-snapshot", op: "snapshot" });
	assert.ok((await laterClient.next()).result.snapshot.imageAdmissionCapability, "the live snapshot is forwarded unchanged");
	assert.equal((await capabilitiesOf(laterClient, "later-after")).attachments.imageInput, false);
	const laterRequestId = randomUUID();
	const laterLease = await writeLease(later, { requestId: laterRequestId, name: "late.png", mediaType: "image/png" });
	laterClient.send(attachmentDispatch(later, laterRequestId, { type: "prompt", message: "x" }, [laterLease.leaseId]));
	assert.equal((await laterClient.next()).error.code, "image_input_unsupported");

	// No environment variable or runtime mode is an input to the gate.
	const previous = { ...process.env };
	process.env.PI_IMAGE_ADMISSION = "1";
	process.env.SESH_PI_IMAGE_INPUT = "true";
	process.env.PI_RPC_SOCKET_IMAGE_INPUT = "strict-v1";
	process.env.NODE_ENV = "test";
	t.after(() => {
		for (const key of ["PI_IMAGE_ADMISSION", "SESH_PI_IMAGE_INPUT", "PI_RPC_SOCKET_IMAGE_INPUT"]) delete process.env[key];
		if (previous.NODE_ENV === undefined) delete process.env.NODE_ENV;
		else process.env.NODE_ENV = previous.NODE_ENV;
	});
	const environment = await admissionFixture(t);
	environment.host.snapshotExtra = inputImageProjection();
	await startAdmission(t, environment, { importedCapability: undefined, prepare: imagePreparer });
	const environmentClient = await JsonlClient.connect(environment.socketPath);
	t.after(() => environmentClient.close());
	assert.equal((await capabilitiesOf(environmentClient, "env-caps")).attachments.imageInput, false);
});

test("input reader and transfer marker rollback can never leave image input advertised", async (t) => {
	// A blocked `.input.sock` keeps text attachments and leaves image readiness false.
	const blocked = await admissionFixture(t);
	const blocker = net.createServer((socket) => socket.destroy());
	await new Promise<void>((resolve, reject) => {
		blocker.once("error", reject);
		blocker.listen(blocked.inputSocketPath, resolve);
	});
	t.after(async () => {
		await new Promise<void>((resolve) => blocker.close(() => resolve()));
	});
	await startAdmission(t, blocked, {
		prepare: async ({ command, files }) => files.some((file) => file.displayName.endsWith(".png"))
			? { ...command, images: [{ type: "image", data: Buffer.from("BLOCKED").toString("base64"), mimeType: "image/png" }] }
			: command,
	});
	assert.ok(blocked.errors.length >= 1, "the reader failure is reported loudly");
	const blockedMarker = JSON.parse(await fs.promises.readFile(path.join(blocked.inbox, "consumer.json"), "utf8"));
	assert.deepEqual(blockedMarker.capabilities, ["attachment-lease-v1"]);
	assert.equal(blockedMarker.input_image_reader, undefined);
	const blockedClient = await JsonlClient.connect(blocked.socketPath);
	t.after(() => blockedClient.close());
	const blockedCapabilities = await capabilitiesOf(blockedClient, "blocked-caps");
	assert.equal(blockedCapabilities.attachments.imageInput, false);
	assert.equal(blockedCapabilities.inputImageBytes, undefined);
	const blockedImageRequest = randomUUID();
	const blockedLease = await writeLease(blocked, { requestId: blockedImageRequest, name: "b.png", mediaType: "image/png" });
	blockedClient.send(attachmentDispatch(blocked, blockedImageRequest, { type: "prompt", message: "x" }, [blockedLease.leaseId]));
	assert.equal((await blockedClient.next()).error.code, "image_input_unsupported");
	const blockedTextRequest = randomUUID();
	const blockedTextLease = await writeLease(blocked, { requestId: blockedTextRequest, name: "t.txt" });
	blockedClient.send(attachmentDispatch(blocked, blockedTextRequest, { type: "prompt", message: "x" }, [blockedTextLease.leaseId]));
	assert.equal((await blockedClient.next()).result.status, "completed");
	assert.equal(blocked.host.dispatchCalls[0]!.argumentCount, 3);

	// A final marker that cannot be published rolls attachment support back entirely.
	const unpublishable = await admissionFixture(t);
	await fs.promises.mkdir(path.join(unpublishable.inbox, "consumer.json"), { mode: 0o700 });
	await startAdmission(t, unpublishable, {});
	assert.ok(unpublishable.errors.length >= 1);
	const unpublishableClient = await JsonlClient.connect(unpublishable.socketPath);
	t.after(() => unpublishableClient.close());
	const unpublishableCapabilities = await capabilitiesOf(unpublishableClient, "rollback-caps");
	assert.equal(unpublishableCapabilities.attachments, undefined, "attachment support is omitted, not downgraded");
	assert.equal(unpublishableCapabilities.inputImageBytes, undefined);
	assert.ok(unpublishableCapabilities.inputImages);
	const rolledBackRequest = randomUUID();
	const rolledBackLease = await writeLease(unpublishable, { requestId: rolledBackRequest, name: "r.png", mediaType: "image/png" });
	unpublishableClient.send(attachmentDispatch(unpublishable, rolledBackRequest, { type: "prompt", message: "x" }, [rolledBackLease.leaseId]));
	assert.equal((await unpublishableClient.next()).error.code, "transfer_unavailable");
	assert.equal(unpublishable.host.dispatchCalls.length, 0);
});

test("managed output startup failure still leaves an exact final attachment and reader marker", async (t) => {
	const fixture = await admissionFixture(t);
	Object.assign(fixture.host, { copyArtifactBytes: async () => ({ byteLength: 0, sha256: "0".repeat(64) }) });
	const imageBase64 = Buffer.from("MANAGED-ROLLBACK-IMAGE").toString("base64");
	await startAdmission(t, fixture, {
		managed: { limits: { maxConcurrentCopies: 3 } },
		prepare: async ({ command }) => ({ ...command, images: [{ type: "image", data: imageBase64, mimeType: "image/png" }] }),
	});
	assert.ok(fixture.errors.length >= 1, "the managed failure is reported");
	const marker = JSON.parse(await fs.promises.readFile(path.join(fixture.inbox, "consumer.json"), "utf8"));
	assert.deepEqual(marker.capabilities, ["attachment-lease-v1", "input-image-read-v1"]);
	const client = await JsonlClient.connect(fixture.socketPath);
	t.after(() => client.close());
	const capabilities = await capabilitiesOf(client, "managed-caps");
	assert.equal(capabilities.managedArtifacts, undefined, "managed output stays absent");
	assert.equal(capabilities.attachments.imageInput, true, "the republished final marker is authoritative");
	const requestId = randomUUID();
	const lease = await writeLease(fixture, { requestId, name: "m.png", mediaType: "image/png" });
	client.send(attachmentDispatch(fixture, requestId, { type: "prompt", message: "x" }, [lease.leaseId]));
	assert.equal((await client.next()).result.status, "completed");
	assert.deepEqual(fixture.host.dispatchCalls[0]!.options, { imageAdmission: "strict-v1" });
});

test("a replaced on-disk listener generation fails the marker ownership check the gate depends on", async (t) => {
	const root = await fs.promises.mkdtemp(path.join(os.tmpdir(), "pi-adm-marker-"));
	t.after(async () => fs.promises.rm(root, { recursive: true, force: true }));
	const inbox = path.join(root, "inbox");
	await fs.promises.mkdir(inbox, { recursive: true, mode: 0o700 });
	await fs.promises.chmod(inbox, 0o700);
	const threadId = randomUUID();
	const target: LiveIdentity = { streamId: "marker-stream", sessionEpoch: 0, sessionId: SESSION_ID };
	const descriptor: LiveInputImageReaderDescriptor = {
		protocol: "pi.input-image-read",
		version: 1,
		listener_generation: randomUUID(),
		socket_suffix: ".input.sock",
	};
	const marker = await initializeLiveTransferMarker(
		{ threadId, inboxPath: inbox, apiSocketPath: path.join(root, "sesh.sock") },
		target,
		["attachment-lease-v1", "input-image-read-v1"],
		descriptor,
	);
	t.after(() => marker.close().catch(() => undefined));
	assert.equal(await marker.matchesInputImageReader(descriptor), true);

	const markerPath = path.join(inbox, "consumer.json");
	const published = JSON.parse(await fs.promises.readFile(markerPath, "utf8"));
	await fs.promises.writeFile(
		markerPath,
		JSON.stringify({
			...published,
			input_image_reader: { ...published.input_image_reader, listener_generation: randomUUID() },
		}),
		{ mode: 0o600 },
	);
	assert.equal(await marker.matchesInputImageReader(descriptor), false, "a replaced generation is not owned");
	await fs.promises.rm(markerPath, { force: true });
	assert.equal(await marker.matchesInputImageReader(descriptor), false, "a removed marker is not owned");
});

test("immutable lease media types are forwarded byte-for-byte and absence stays absent", async (t) => {
	const fixture = await admissionFixture(t);
	await startAdmission(t, fixture, {});
	const client = await JsonlClient.connect(fixture.socketPath);
	t.after(() => client.close());

	const requestId = randomUUID();
	const verbatim = "image/PNG; charset=US-ASCII";
	const first = await writeLease(fixture, { requestId, name: "first.txt", mediaType: verbatim });
	const second = await writeLease(fixture, { requestId, name: "second.txt", mediaType: "" });
	const third = await writeLease(fixture, { requestId, name: "third.txt", mediaType: null });
	client.send(attachmentDispatch(fixture, requestId, { type: "prompt", message: "mime" }, [
		first.leaseId,
		second.leaseId,
		third.leaseId,
	]));
	assert.equal((await client.next()).result.status, "completed");

	const files = fixture.preparations[0]!.files;
	assert.deepEqual(files.map((file) => file.displayName), ["first.txt", "second.txt", "third.txt"]);
	assert.equal(files[0]!.declaredMediaType, verbatim, "no trimming, lower-casing or canonicalization");
	assert.equal(files[1]!.declaredMediaType, "", "an explicitly empty declaration stays empty");
	assert.equal(Object.hasOwn(files[2]!, "declaredMediaType"), false, "an absent declaration stays absent");
	assert.equal(files[1]!.declaredMediaType === undefined, false, "empty and absent remain distinct");
});

test("an attachment-bearing dispatch refuses caller command.images before any reservation", async (t) => {
	const fixture = await admissionFixture(t);
	await startAdmission(t, fixture, {});
	const client = await JsonlClient.connect(fixture.socketPath);
	t.after(() => client.close());

	const requestId = randomUUID();
	const lease = await writeLease(fixture, { requestId, name: "notes.txt" });
	const callerImages = [{ type: "image", data: Buffer.from("CALLER-SUPPLIED-BASE64").toString("base64"), mimeType: "image/png" }];
	for (const images of [[], callerImages]) {
		client.send(attachmentDispatch(fixture, requestId, { type: "prompt", message: "merge", images }, [lease.leaseId]));
		const rejected = await client.next();
		assert.equal(rejected.ok, false);
		assert.equal(rejected.error.code, "invalid_request");
		assert.match(rejected.error.message, /command\.images/);
	}
	assert.equal(fixture.preparations.length, 0, "no lease read, scratch copy or helper call happened");
	assert.equal(fixture.host.dispatchCalls.length, 0);
	assert.equal((await getResult(client, fixture, "images-result", requestId)).error.code, "result_not_found");

	// The identity was never reserved, so the clean request with the same ID is accepted.
	client.send(attachmentDispatch(fixture, requestId, { type: "prompt", message: "clean" }, [lease.leaseId]));
	assert.equal((await client.next()).result.status, "completed");
	assert.equal(fixture.preparations.length, 1);

	// Ordinary non-attachment native images keep their existing behavior.
	const legacyId = "legacy-images-1";
	client.send({
		protocol: "live-v1",
		id: legacyId,
		op: "dispatch",
		target: fixture.host.identity,
		command: { type: "prompt", message: "legacy", images: callerImages },
	});
	assert.equal((await client.next()).result.status, "completed");
	assert.deepEqual((fixture.host.dispatchCalls.at(-1)!.command as { images: unknown }).images, callerImages);
	assert.equal(fixture.host.dispatchCalls.at(-1)!.argumentCount, 3);
});

test("only exact native preparation codes are echoed, and never native prose", async (t) => {
	const fixture = await admissionFixture(t);
	const secret = "/tmp/scratch/0 decoder=libwebp declared=image/webp";
	let failure: unknown;
	await startAdmission(t, fixture, {
		prepare: async () => {
			throw failure;
		},
	});
	const client = await JsonlClient.connect(fixture.socketPath);
	t.after(() => client.close());

	const expectations: Array<[unknown, string]> = [
		...PREPARATION_CODES.map((code) => [
			Object.assign(new Error(`native failure ${secret}`), { code }),
			`Native attachment preparation failed (${code})`,
		] as [unknown, string]),
		[Object.assign(new Error(`native failure ${secret}`), { code: "not_an_allowlisted_code" }), "Native attachment preparation failed"],
		[Object.assign(new Error(`native failure ${secret}`), { code: 42 }), "Native attachment preparation failed"],
		[new Error(`native failure ${secret}`), "Native attachment preparation failed"],
		[`raw string failure ${secret}`, "Native attachment preparation failed"],
		[{ message: `object failure ${secret}`, phase: "preparation" }, "Native attachment preparation failed"],
	];

	for (const [thrown, expected] of expectations) {
		failure = thrown;
		const requestId = randomUUID();
		const lease = await writeLease(fixture, { requestId, name: "subject.png", mediaType: "image/png" });
		client.send(attachmentDispatch(fixture, requestId, { type: "prompt", message: "prepare" }, [lease.leaseId]));
		const response = await client.next();
		assert.equal(response.ok, false, expected);
		assert.equal(response.error.code, "attachment_prepare_failed", expected);
		assert.equal(response.error.message, expected);
		const retained = await getResult(client, fixture, `prep-${requestId}`, requestId);
		assert.equal(retained.result.status, "failed");
		assert.equal(retained.result.error.code, "attachment_prepare_failed");
		assert.equal(retained.result.error.message, expected);
		assert.ok(!JSON.stringify([response, retained]).includes("/tmp/scratch"), "no path or native prose is retained");
		assert.ok(!JSON.stringify([response, retained]).includes("libwebp"));
		client.send(attachmentDispatch(fixture, requestId, { type: "prompt", message: "retry" }, [lease.leaseId]));
		assert.equal((await client.next()).error.code, "duplicate_request", "a preparation failure is not replayable");
	}
	assert.equal(fixture.host.dispatchCalls.length, 0, "a preparation failure never reaches native dispatch");
});

const IMAGE_BASE64 = Buffer.from("STRICT-DISPATCH-IMAGE-BYTES").toString("base64");

const strictImagePreparer: PrepareRpcPromptFiles = async ({ command }) => ({
	...command,
	images: [{ type: "image", data: IMAGE_BASE64, mimeType: "image/png" }],
});

async function dispatchStrictImage(
	client: JsonlClient,
	fixture: AdmissionFixture,
	message = "inspect",
): Promise<{ requestId: string; response: any }> {
	const requestId = randomUUID();
	const lease = await writeLease(fixture, { requestId, name: "subject.png", mediaType: "image/png" });
	client.send(attachmentDispatch(fixture, requestId, { type: "prompt", message }, [lease.leaseId]));
	return { requestId, response: await client.next() };
}

test("all thirteen admission phase and code pairs are retained as definitive rejections", async (t) => {
	const fixture = await admissionFixture(t);
	const prose = "native admission prose /tmp/scratch/0 model=claude";
	let rejection: unknown;
	fixture.host.dispatchImpl = async () => {
		throw rejection;
	};
	await startAdmission(t, fixture, { prepare: strictImagePreparer });
	const client = await JsonlClient.connect(fixture.socketPath);
	t.after(() => client.close());
	assert.equal((await capabilitiesOf(client, "admission-caps")).attachments.imageInput, true);

	for (const code of ADMISSION_CODES) {
		rejection = Object.assign(new Error(prose), { code, phase: "admission" });
		const { requestId, response } = await dispatchStrictImage(client, fixture, code);
		assert.equal(response.ok, false, code);
		assert.equal(response.error.code, "image_admission_rejected", code);
		assert.equal(response.error.message, `Native image admission rejected (${code})`, code);
		const retained = await getResult(client, fixture, `adm-${requestId}`, requestId);
		assert.equal(retained.result.status, "failed", code);
		assert.equal(retained.result.error.code, "image_admission_rejected", code);
		assert.equal(retained.result.error.message, `Native image admission rejected (${code})`, code);
		assert.equal(typeof retained.result.finishedAt, "number");
		const observable = JSON.stringify([response, retained]);
		assert.ok(!observable.includes("/tmp/scratch"), code);
		assert.ok(!observable.includes("claude"), code);
		assert.ok(!observable.includes(IMAGE_BASE64), code);
		client.send(attachmentDispatch(fixture, requestId, { type: "prompt", message: code }, [randomUUID()]));
		assert.equal((await client.next()).error.code, "duplicate_request", code);
	}
	assert.equal(errorText(fixture.errors).includes("more than one result"), false, "a rejection produces no second settle");
	assert.deepEqual(fixture.errors, []);
});

test("request-phase and mismatched pairs stay unresolved instead of becoming admission rejections", async (t) => {
	const fixture = await admissionFixture(t);
	let rejection: unknown;
	fixture.host.dispatchImpl = async () => {
		throw rejection;
	};
	const timestamps = [1_700_000_000_000, 1_700_000_000_001];
	let tick = 0;
	await startAdmission(t, fixture, {
		prepare: strictImagePreparer,
		now: () => timestamps[Math.min(tick++, timestamps.length - 1)]!,
	});
	const client = await JsonlClient.connect(fixture.socketPath);
	t.after(() => client.close());

	const variants: Array<[string, unknown]> = [
		...REQUEST_CODES.map((code) => [
			`request/${code}`,
			Object.assign(new Error(`request phase ${code}`), { code, phase: "request" }),
		] as [string, unknown]),
		["admission/unknown-code", Object.assign(new Error("mismatch"), { code: "image_virtual_route_incompatible", phase: "admission" })],
		["request/admission-code", Object.assign(new Error("mismatch"), { code: "image_admission_cancelled", phase: "request" })],
		["code-without-phase", Object.assign(new Error("mismatch"), { code: "image_input_blocked" })],
		["phase-without-code", Object.assign(new Error("mismatch"), { phase: "admission" })],
		["wrong-phase-type", Object.assign(new Error("mismatch"), { code: "image_input_blocked", phase: ["admission"] })],
	];

	for (const [label, thrown] of variants) {
		rejection = thrown;
		const { requestId, response } = await dispatchStrictImage(client, fixture, label);
		assert.equal(response.ok, true, label);
		assert.deepEqual(Object.keys(response.result).sort(), ["finishedAt", "reason", "requestId", "status"], label);
		assert.equal(response.result.status, "uncertain", label);
		assert.equal(response.result.reason, "native_dispatch_rejected", label);
		assert.equal(response.result.requestId, requestId, label);
		assert.ok(Number.isFinite(response.result.finishedAt) && response.result.finishedAt >= 0, label);
		const retained = await getResult(client, fixture, `req-${requestId}`, requestId);
		assert.deepEqual(retained.result, response.result, label);
		assert.ok(!JSON.stringify([response, retained]).includes("image_admission_rejected"), label);
		assert.ok(!JSON.stringify([response, retained]).includes("mismatch"), label);
	}
});

test("an unresolved strict rejection is retained exactly, redacted, scoped and never replayable", async (t) => {
	const fixture = await admissionFixture(t);
	// Embeds an exact refusal phrase: a substring collision must not be promoted to a
	// definitive classification, and must not echo the surrounding prose.
	const sentinel = "RAW-NATIVE-SENTINEL Runtime is failed /tmp/scratch/0 base64=AAAA";
	fixture.host.dispatchImpl = async () => {
		throw new Error(sentinel);
	};
	const finishedAt = 1_762_000_000_123;
	await startAdmission(t, fixture, { prepare: strictImagePreparer, now: () => finishedAt });
	const client = await JsonlClient.connect(fixture.socketPath);
	t.after(() => client.close());

	const { requestId, response } = await dispatchStrictImage(client, fixture);
	assert.deepEqual(response.result, {
		requestId,
		status: "uncertain",
		reason: "native_dispatch_rejected",
		finishedAt,
	});
	assert.equal(response.ok, true, "successful delivery of an unresolved result");

	const retained = await getResult(client, fixture, "uncertain-result", requestId);
	assert.deepEqual(retained.result, response.result);
	const observable = JSON.stringify([response, retained]);
	assert.ok(!observable.includes("RAW-NATIVE-SENTINEL"));
	assert.ok(!observable.includes("Runtime is failed"));
	assert.ok(!observable.includes("/tmp/scratch"));
	assert.ok(!observable.includes("base64=AAAA"));
	assert.ok(!observable.includes(IMAGE_BASE64));
	assert.equal(response.error, undefined, "an unresolved submission is never a definitive failure");

	// The diagnostic names the request, never the native exception text.
	const diagnostics = errorText(fixture.errors);
	assert.ok(diagnostics.includes(requestId), diagnostics);
	assert.ok(diagnostics.includes("unresolved"), diagnostics);
	assert.ok(!diagnostics.includes("RAW-NATIVE-SENTINEL"), diagnostics);
	assert.ok(!diagnostics.includes("Runtime is failed"), diagnostics);
	assert.ok(!diagnostics.includes("/tmp/scratch"), diagnostics);
	assert.ok(!diagnostics.includes("base64=AAAA"), diagnostics);

	client.send(attachmentDispatch(fixture, requestId, { type: "prompt", message: "replay" }, [randomUUID()]));
	assert.equal((await client.next()).error.code, "duplicate_request");
	client.send({
		protocol: "live-v1",
		id: "uncertain-mismatch",
		op: "get_result",
		target: { ...fixture.host.identity, sessionEpoch: fixture.host.identity.sessionEpoch + 1 },
		requestId,
	});
	assert.equal((await client.next()).error.code, "result_target_mismatch");
	assert.equal(fixture.host.dispatchCalls.length, 1, "the mutation is never automatically retried");
});

test("only exact source-grounded refusals stay definitive, with fixed safe text", async (t) => {
	const fixture = await admissionFixture(t);
	let rejection: unknown;
	fixture.host.dispatchImpl = async () => {
		throw rejection;
	};
	await startAdmission(t, fixture, { prepare: strictImagePreparer });
	const client = await JsonlClient.connect(fixture.socketPath);
	t.after(() => client.close());

	// Exactly the strings Pi's standard-user dispatch path can throw.
	const refusals: Array<[string, string, string]> = [
		["Live RPC target is stale; reconcile the session before submitting", "stale_target", "stale target"],
		["Runtime is replacing", "runtime_unavailable", "runtime replacing"],
		["Runtime is failed", "runtime_unavailable", "runtime failed"],
		["Runtime is disposed", "runtime_unavailable", "runtime disposed"],
	];
	const fixedMessages: Record<string, string> = {
		stale_target: "Live dispatch target is stale; reconcile the session before resubmitting",
		runtime_unavailable: "Live runtime is unavailable; reconcile the session before resubmitting",
	};

	for (const [native, code, label] of refusals) {
		fixture.errors.length = 0;
		rejection = new Error(native);
		const { requestId, response } = await dispatchStrictImage(client, fixture, code);
		assert.equal(response.ok, false, native);
		assert.equal(response.error.code, code, native);
		assert.equal(response.error.message, fixedMessages[code], native);
		assert.notEqual(response.error.message, native, "native prose is replaced by fixed socket text");
		const retained = await getResult(client, fixture, `refusal-${requestId}`, requestId);
		assert.equal(retained.result.status, "failed", native);
		assert.equal(retained.result.error.code, code, native);
		assert.equal(retained.result.error.message, fixedMessages[code], native);
		// The diagnostic distinguishes the refusal with a socket-owned label only.
		const diagnostics = errorText(fixture.errors);
		assert.ok(diagnostics.includes(`was refused: ${label}`), diagnostics);
		assert.ok(diagnostics.includes(requestId), diagnostics);
		client.send(attachmentDispatch(fixture, requestId, { type: "prompt", message: code }, [randomUUID()]));
		assert.equal((await client.next()).error.code, "duplicate_request", native);
	}
});

test("refusal-phrase collisions and unreadable errors stay unresolved", async (t) => {
	const fixture = await admissionFixture(t);
	let rejection: unknown;
	fixture.host.dispatchImpl = async () => {
		throw rejection;
	};
	const finishedAt = 1_762_000_777_000;
	await startAdmission(t, fixture, { prepare: strictImagePreparer, now: () => finishedAt });
	const client = await JsonlClient.connect(fixture.socketPath);
	t.after(() => client.close());

	const throwingMessage = new Error("placeholder");
	Object.defineProperty(throwingMessage, "message", {
		get() {
			throw new Error("ACCESSOR-LEAK-MESSAGE /tmp/secret");
		},
	});
	const throwingCode = new Error("Runtime is failed");
	Object.defineProperty(throwingCode, "code", {
		get() {
			throw new Error("ACCESSOR-LEAK-CODE /tmp/secret");
		},
	});
	const throwingPhase = Object.assign(new Error("Runtime is failed"), { code: "image_input_blocked" });
	Object.defineProperty(throwingPhase, "phase", {
		get() {
			throw new Error("ACCESSOR-LEAK-PHASE /tmp/secret");
		},
	});

	const collisions: Array<[string, unknown]> = [
		// Substring, prefix, suffix, whitespace and case collisions.
		["embedded", new Error("LEAK-SENTINEL Runtime is failed /tmp/scratch/0 base64=AAAA")],
		["prefixed", new Error("Runtime is failedx")],
		["suffixed", new Error("xRuntime is failed")],
		["trailing space", new Error("Runtime is failed ")],
		["leading space", new Error(" Runtime is failed")],
		["lower case", new Error("runtime is failed")],
		["extended refusal", new Error("Runtime is replacing the live session")],
		["paraphrased stale", new Error("Live dispatch target is stale")],
		["partial stale", new Error("Live RPC target is stale")],
		["newline wrapped", new Error("Runtime is failed\nLEAK-SENTINEL /tmp/scratch/0")],
		// Real native strings that are not standard-user dispatch refusals.
		["artifact stale", new Error("Live RPC artifact target is stale; reconcile the session before copying")],
		["input-image stale", new Error("Live RPC input-image target is stale; reconcile the session before reading")],
		["closed observer", new Error("Live RPC observer is closed")],
		["ready state unknown", new Error("Runtime is ready")],
		// Typed errors whose prose matches a refusal cannot be promoted by that prose.
		["request phase collision", Object.assign(new Error("Runtime is failed"), { code: "image_input_blocked", phase: "request" })],
		["unknown admission code collision", Object.assign(new Error("Runtime is failed"), { code: "image_virtual_route_incompatible", phase: "admission" })],
		["code without phase collision", Object.assign(new Error("Runtime is failed"), { code: "image_input_blocked" })],
		["phase without code collision", Object.assign(new Error("Live RPC target is stale; reconcile the session before submitting"), { phase: "admission" })],
		// Non-Error values are never stringified during classification.
		["string value", "Runtime is failed"],
		["plain object", { message: "Runtime is failed", stack: "LEAK-SENTINEL /tmp/secret" }],
		["null", null],
		["number", 42],
		// Failing property reads classify conservatively and never leak.
		["throwing message accessor", throwingMessage],
		["throwing code accessor", throwingCode],
		["throwing phase accessor", throwingPhase],
	];

	for (const [label, thrown] of collisions) {
		fixture.errors.length = 0;
		rejection = thrown;
		const { requestId, response } = await dispatchStrictImage(client, fixture, label);
		assert.equal(response.ok, true, label);
		assert.deepEqual(response.result, {
			requestId,
			status: "uncertain",
			reason: "native_dispatch_rejected",
			finishedAt,
		}, label);
		const retained = await getResult(client, fixture, `collision-${requestId}`, requestId);
		assert.deepEqual(retained.result, response.result, label);
		const observable = `${JSON.stringify([response, retained])}\n${errorText(fixture.errors)}`;
		for (const secret of [
			"LEAK-SENTINEL",
			"ACCESSOR-LEAK",
			"/tmp/secret",
			"/tmp/scratch",
			"base64=AAAA",
			"Runtime is",
			"target is stale",
			"stale_target",
			"runtime_unavailable",
			"image_admission_rejected",
			"was refused",
		]) {
			assert.ok(!observable.includes(secret), `${label} must not expose ${secret}`);
		}
		assert.ok(errorText(fixture.errors).includes("unresolved"), label);
		client.send(attachmentDispatch(fixture, requestId, { type: "prompt", message: label }, [randomUUID()]));
		assert.equal((await client.next()).error.code, "duplicate_request", label);
	}

	// Precedence: an exact admission pair stays definitive even when its own prose
	// matches a refusal string.
	fixture.errors.length = 0;
	rejection = Object.assign(new Error("Runtime is failed"), { code: "image_input_blocked", phase: "admission" });
	const { requestId, response } = await dispatchStrictImage(client, fixture, "admission precedence");
	assert.equal(response.ok, false);
	assert.equal(response.error.code, "image_admission_rejected");
	assert.equal(response.error.message, "Native image admission rejected (image_input_blocked)");
	assert.ok(!JSON.stringify(response).includes("Runtime is"));
	assert.equal((await getResult(client, fixture, `precedence-${requestId}`, requestId)).result.error.code, "image_admission_rejected");
});

test("an accepted strict submission stays accepted when the host rejects afterwards", async (t) => {
	const fixture = await admissionFixture(t);
	const sentinel = "LATE-RAW-REJECTION /tmp/scratch/0";
	fixture.host.dispatchImpl = async (call, output) => {
		output({ type: (call.command as { type: string }).type, success: true, disposition: "started" });
		throw new Error(sentinel);
	};
	await startAdmission(t, fixture, { prepare: strictImagePreparer });
	const client = await JsonlClient.connect(fixture.socketPath);
	t.after(() => client.close());

	const { requestId, response } = await dispatchStrictImage(client, fixture);
	assert.equal(response.ok, true);
	assert.equal(response.result.status, "completed");
	assert.deepEqual(response.result.response, { type: "prompt", success: true, disposition: "started" });

	const retained = await getResult(client, fixture, "accepted-result", requestId);
	assert.equal(retained.result.status, "completed", "the accepted result is never rewritten");
	assert.deepEqual(retained.result.response, response.result.response);

	// A later ordinary run failure is reported through Pi events and changes nothing.
	fixture.host.emit("agent_error", { error: { message: "later provider failure" } });
	const afterEvent = await getResult(client, fixture, "accepted-after-event", requestId);
	assert.equal(afterEvent.result.status, "completed");

	const diagnostics = errorText(fixture.errors);
	assert.ok(diagnostics.includes("after settlement"), diagnostics);
	assert.ok(diagnostics.includes(requestId), diagnostics);
	assert.ok(!diagnostics.includes("LATE-RAW-REJECTION"), diagnostics);
	assert.ok(!diagnostics.includes("more than one result"), diagnostics);
});

test("the unresolved record is reserved upfront and survives result capacity pressure", async (t) => {
	const fixture = await admissionFixture(t);
	fixture.host.dispatchImpl = async () => {
		throw new Error("capacity-pressure rejection");
	};
	const finishedAt = 1_762_000_111_222;
	await startAdmission(t, fixture, {
		prepare: strictImagePreparer,
		now: () => finishedAt,
		limits: { maxResults: 1, maxResultBytes: 4096 },
	});
	const client = await JsonlClient.connect(fixture.socketPath);
	t.after(() => client.close());

	const { requestId, response } = await dispatchStrictImage(client, fixture);
	assert.deepEqual(response.result, {
		requestId,
		status: "uncertain",
		reason: "native_dispatch_rejected",
		finishedAt,
	});
	const retained = await getResult(client, fixture, "capacity-result", requestId);
	assert.deepEqual(retained.result, response.result, "retention cannot be downgraded to a tombstone");

	const second = randomUUID();
	const lease = await writeLease(fixture, { requestId: second, name: "second.png", mediaType: "image/png" });
	client.send(attachmentDispatch(fixture, second, { type: "prompt", message: "second" }, [lease.leaseId]));
	assert.equal((await client.next()).error.code, "capacity");
	assert.equal(fixture.errors.length, 1, errorText(fixture.errors));
	assert.ok(errorText(fixture.errors).includes("unresolved"), "only the redacted uncertainty diagnostic is reported");
});

test("non-strict dispatch keeps its existing bounded internal_error behavior", async (t) => {
	const fixture = await admissionFixture(t);
	let rejection: unknown = new Error("ordinary unbounded host failure with context");
	fixture.host.dispatchImpl = async () => {
		throw rejection;
	};
	await startAdmission(t, fixture, {});
	const client = await JsonlClient.connect(fixture.socketPath);
	t.after(() => client.close());

	client.send({
		protocol: "live-v1",
		id: "plain-dispatch",
		op: "dispatch",
		target: fixture.host.identity,
		command: { type: "prompt", message: "plain" },
	});
	const plain = await client.next();
	assert.equal(plain.ok, false);
	assert.equal(plain.error.code, "internal_error");
	assert.equal(plain.error.message, "ordinary unbounded host failure with context");

	// A text-only attachment dispatch is not a strict image dispatch either.
	const requestId = randomUUID();
	const lease = await writeLease(fixture, { requestId, name: "notes.txt" });
	client.send(attachmentDispatch(fixture, requestId, { type: "prompt", message: "text" }, [lease.leaseId]));
	const text = await client.next();
	assert.equal(text.error.code, "internal_error");
	assert.equal(text.error.message, "ordinary unbounded host failure with context");
	const retained = await getResult(client, fixture, "plain-result", requestId);
	assert.equal(retained.result.status, "failed");
	assert.equal(retained.result.error.code, "internal_error");
	assert.equal(fixture.host.dispatchCalls.every((call) => call.argumentCount === 3), true);

	// The strict-only classification change leaves the legacy substring classification
	// and its bounded echo exactly as they were for every non-strict command.
	for (const [message, code] of [
		["ordinary failure where Runtime is failed appears mid-sentence", "runtime_unavailable"],
		["ordinary failure where the target is stale appears mid-sentence", "stale_target"],
		["runtime is DISPOSED in a lower-case paraphrase", "runtime_unavailable"],
	] as const) {
		rejection = new Error(message);
		const legacyId = `legacy-${code}-${message.length}`;
		client.send({
			protocol: "live-v1",
			id: legacyId,
			op: "dispatch",
			target: fixture.host.identity,
			command: { type: "prompt", message: "legacy" },
		});
		const legacy = await client.next();
		assert.equal(legacy.error.code, code, message);
		assert.equal(legacy.error.message, message, "non-strict prose is still echoed verbatim");
	}
});

test("raw lease digests are verified before preparation and never correlated with native bytes", async (t) => {
	const fixture = await admissionFixture(t);
	const sourceBytes = Buffer.from("BM-RAW-BITMAP-SOURCE-BYTES");
	const convertedBase64 = Buffer.from("PNG-CONVERTED-BY-NATIVE-PREPARATION").toString("base64");
	let observedFiles: readonly string[] = [];
	await startAdmission(t, fixture, {
		prepare: async ({ command, files }) => {
			observedFiles = files.map((file) => file.path);
			assert.deepEqual(
				await Promise.all(files.map((file) => fs.promises.readFile(file.path))),
				[sourceBytes],
				"the verified raw bytes reach the helper unchanged",
			);
			return { ...command, images: [{ type: "image", data: convertedBase64, mimeType: "image/png" }] };
		},
	});
	const client = await JsonlClient.connect(fixture.socketPath);
	t.after(() => client.close());
	client.send({ protocol: "live-v1", id: "digest-sub", op: "subscribe" });
	await client.next();
	await client.next();

	// A corrupt payload digest fails before the helper is ever invoked.
	const corruptRequestId = randomUUID();
	const corrupt = await writeLease(fixture, { requestId: corruptRequestId, name: "corrupt.bmp", bytes: sourceBytes, mediaType: "image/bmp" });
	await fs.promises.writeFile(path.join(fixture.inbox, "leases", corrupt.leaseId, "payload"), Buffer.from("BM-TAMPERED-SOURCE-BYTES!"), { mode: 0o600 });
	client.send(attachmentDispatch(fixture, corruptRequestId, { type: "prompt", message: "corrupt" }, [corrupt.leaseId]));
	assert.equal((await client.next()).error.code, "lease_corrupt");
	assert.equal(fixture.preparations.length, 0, "raw digest verification precedes native preparation");
	assert.equal(fixture.host.dispatchCalls.length, 0);

	// A converted image is admitted without any digest comparison or identity invention.
	const requestId = randomUUID();
	const lease = await writeLease(fixture, { requestId, name: "diagram.bmp", bytes: sourceBytes, mediaType: "image/bmp" });
	client.send(attachmentDispatch(fixture, requestId, { type: "prompt", message: "convert" }, [lease.leaseId]));
	const response = await client.next();
	assert.equal(response.result.status, "completed");
	assert.equal(fixture.preparations.length, 1, "exactly one preparation");
	assert.equal(fixture.host.dispatchCalls.length, 1, "exactly one dispatch");
	assert.equal(observedFiles.every((file) => !fs.existsSync(file)), true, "verified scratch copies are removed");
	assert.deepEqual(await fs.promises.readFile(path.join(fixture.inbox, "leases", lease.leaseId, "payload")), sourceBytes, "the Sesh lease payload is untouched");

	const retained = await getResult(client, fixture, "digest-result", requestId);
	const observable = JSON.stringify([response, retained]);
	for (const secret of [lease.sha256, convertedBase64, sourceBytes.toString("base64"), "inputImageId", "declaredMediaType", "image/bmp"]) {
		assert.ok(!observable.includes(secret), `the transport must not expose ${secret.slice(0, 20)}`);
	}
	// The socket adds no event of its own for an admitted image.
	await assert.rejects(client.next(250), /timed out/);
});

test("one startup state controls both the advertised capability and strict dispatch", async (t) => {
	const fixture = await admissionFixture(t);
	const imageBase64 = Buffer.from("CONSUMER-STATE-IMAGE").toString("base64");
	const preparations: PrepareRpcPromptFilesInput[] = [];
	const prepareRpcPromptFiles: PrepareRpcPromptFiles = async (input) => {
		preparations.push(input);
		return input.files.some((file) => file.displayName.endsWith(".png"))
			? { ...input.command, images: [{ type: "image", data: imageBase64, mimeType: "image/png" }] }
			: input.command;
	};
	const consumerOptions = {
		threadId: fixture.threadId,
		inboxPath: fixture.inbox,
		apiSocketPath: fixture.apiSocketPath,
		prepareRpcPromptFiles,
		markerOwner: "external" as const,
	};
	const dispatchThrough = async (
		consumer: { prepare: (...args: any[]) => Promise<{ command: Record<string, unknown>; strictImageAdmission: boolean }> },
		name: string,
	) => {
		const requestId = randomUUID();
		const lease = await writeLease(fixture, { requestId, name });
		return await consumer.prepare(requestId, fixture.host.identity, { type: "prompt", message: name }, [{ leaseId: lease.leaseId }]);
	};

	const strict = await initializeLiveAttachmentConsumer(
		{ ...consumerOptions, strictImagePreparation: true },
		fixture.host.identity,
		fixture.root,
	);
	t.after(() => strict.close());
	assert.equal(strict.strictImagePreparation, true);
	assert.equal(strict.capability.imageInput, false, "strict preparation support alone advertises nothing");
	assert.equal((await dispatchThrough(strict, "notes.txt")).strictImageAdmission, false);
	assert.equal(preparations.at(-1)!.imageAdmission, "strict-v1");
	await assert.rejects(dispatchThrough(strict, "photo.png"), /no prompt was dispatched/);

	strict.activateImageInput();
	assert.equal(strict.capability.imageInput, true);
	const admitted = await dispatchThrough(strict, "photo.png");
	assert.equal(admitted.strictImageAdmission, true);
	assert.deepEqual(admitted.command.images, [{ type: "image", data: imageBase64, mimeType: "image/png" }]);
	assert.equal((await dispatchThrough(strict, "notes.txt")).strictImageAdmission, false, "text stays non-strict after activation");
	assert.throws(() => strict.activateImageInput(), /already been activated/);

	const generic = await initializeLiveAttachmentConsumer(
		{ ...consumerOptions },
		fixture.host.identity,
		fixture.root,
	);
	t.after(() => generic.close());
	assert.equal(generic.strictImagePreparation, false);
	assert.throws(() => generic.activateImageInput(), /exact strict native image-admission support/);
	assert.equal(generic.capability.imageInput, false);
	assert.equal((await dispatchThrough(generic, "notes.txt")).strictImageAdmission, false);
	assert.equal(preparations.at(-1)!.imageAdmission, undefined);
	await assert.rejects(dispatchThrough(generic, "photo.png"), /no prompt was dispatched/);
});

test("successful managed output coexists with image input without changing its semantics", async (t) => {
	const fixture = await admissionFixture(t);
	Object.assign(fixture.host, { copyArtifactBytes: async () => ({ byteLength: 0, sha256: "0".repeat(64) }) });
	const imageBase64 = Buffer.from("MANAGED-COEXISTENCE-IMAGE").toString("base64");
	await startAdmission(t, fixture, {
		managed: {},
		prepare: async ({ command }) => ({ ...command, images: [{ type: "image", data: imageBase64, mimeType: "image/png" }] }),
	});
	assert.deepEqual(fixture.errors, []);
	const marker = JSON.parse(await fs.promises.readFile(path.join(fixture.inbox, "consumer.json"), "utf8"));
	assert.deepEqual(marker.capabilities, ["attachment-lease-v1", "artifact-publish-v1", "input-image-read-v1"]);
	const client = await JsonlClient.connect(fixture.socketPath);
	t.after(() => client.close());
	const capabilities = await capabilitiesOf(client, "managed-ok-caps");
	assert.ok(capabilities.managedArtifacts);
	assert.equal(capabilities.attachments.imageInput, true);
	const requestId = randomUUID();
	const lease = await writeLease(fixture, { requestId, name: "coexist.png", mediaType: "image/png" });
	client.send(attachmentDispatch(fixture, requestId, { type: "prompt", message: "x" }, [lease.leaseId]));
	assert.equal((await client.next()).result.status, "completed");
	assert.deepEqual(fixture.host.dispatchCalls[0]!.options, { imageAdmission: "strict-v1" });
});
