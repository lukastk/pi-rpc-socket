import { test, type TestContext } from "node:test";
import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as http from "node:http";
import * as net from "node:net";
import * as os from "node:os";
import * as path from "node:path";
import { createHash, randomUUID } from "node:crypto";
import {
	MANAGED_ARTIFACT_CAPABILITY,
	createManagedArtifactSeshClient,
	deriveManagedArtifactClientId,
	startManagedArtifactPublisher,
	type ArtifactCapableLiveTransportHost,
	type ManagedArtifactResolveRequest,
	type ManagedArtifactRuntimeMapping,
	type ManagedArtifactSeshClient,
	type NativeArtifactProjection,
} from "../managed-artifact-publisher.ts";
import { startLiveTransport, type LiveEvent, type LiveIdentity, type LiveSnapshot } from "../live-transport.ts";
import { LIVE_INPUT_IMAGE_READER_CAPABILITY } from "../input-image-reader.ts";

const THREAD_ID = "550e8400-e29b-41d4-a716-446655440000";
const SESSION_ID = "123e4567-e89b-12d3-a456-426614174000";
const PUBLICATION_ID = "98765432-1234-4abc-8def-0123456789ab";

const delay = (milliseconds: number) => new Promise((resolve) => setTimeout(resolve, milliseconds));

async function waitFor(predicate: () => boolean, timeoutMs = 3000): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	while (!predicate()) {
		if (Date.now() >= deadline) throw new Error("timed out waiting for managed artifact condition");
		await delay(10);
	}
}

function projection(
	publicationId = PUBLICATION_ID,
	bytes = Buffer.from("managed artifact bytes"),
	overrides: Partial<NativeArtifactProjection> = {},
): NativeArtifactProjection {
	return {
		publicationId,
		kind: "image",
		displayName: "output.png",
		mediaType: "image/png",
		byteLength: bytes.length,
		sha256: createHash("sha256").update(bytes).digest("hex"),
		disposition: "inline",
		retention: "runtime_source",
		origin: "native",
		availability: { status: "available" },
		...overrides,
	};
}

class ArtifactHost implements ArtifactCapableLiveTransportHost {
	identity: LiveIdentity;
	sequence = 0;
	artifacts: NativeArtifactProjection[];
	bytes = new Map<string, Buffer>();
	copyCalls: Array<{ target: LiveIdentity; publicationId: string; options: Record<string, unknown> }> = [];
	dispatchCalls: unknown[] = [];
	copyImpl?: ArtifactHost["copyArtifactBytes"];
	private listeners = new Set<(event: LiveEvent) => void>();

	constructor(identity: LiveIdentity, artifacts: NativeArtifactProjection[] = []) {
		this.identity = identity;
		this.artifacts = artifacts;
	}

	snapshot(): LiveSnapshot {
		return {
			...this.identity,
			sequence: this.sequence,
			artifacts: this.artifacts,
			pendingArtifacts: [],
			artifactCoverage: {
				scope: "current_branch_artifacts",
				leafId: null,
				startIndex: 0,
				endIndexExclusive: this.artifacts.length,
				totalArtifacts: this.artifacts.length,
				byteLength: 2,
				hasOlder: false,
				hasNewer: false,
			},
		} as LiveSnapshot;
	}

	subscribe(listener: (event: LiveEvent) => void): () => void {
		this.listeners.add(listener);
		return () => this.listeners.delete(listener);
	}

	async dispatch(_target: LiveIdentity, command: unknown): Promise<unknown> {
		this.dispatchCalls.push(command);
		return {
			scope: "current_branch_artifacts",
			artifacts: [],
			coverage: {
				scope: "current_branch_artifacts",
				leafId: null,
				startIndex: 0,
				endIndexExclusive: 0,
				totalArtifacts: 0,
				byteLength: 2,
				hasOlder: false,
				hasNewer: false,
			},
		};
	}

	async copyArtifactBytes(
		target: LiveIdentity,
		publicationId: string,
		options: { maxBytes: number; chunkBytes: number; timeoutMs: number; signal?: AbortSignal },
		write: (chunk: Uint8Array, offset: number) => void | Promise<void>,
	): Promise<{ byteLength: number; sha256: string }> {
		if (this.copyImpl) return await this.copyImpl(target, publicationId, options, write);
		const bytes = this.bytes.get(publicationId);
		if (!bytes) throw Object.assign(new Error("not found"), { code: "artifact_not_found" });
		this.copyCalls.push({ target, publicationId, options });
		for (let offset = 0; offset < bytes.length; offset += options.chunkBytes) {
			await write(bytes.subarray(offset, Math.min(bytes.length, offset + options.chunkBytes)), offset);
		}
		return { byteLength: bytes.length, sha256: createHash("sha256").update(bytes).digest("hex") };
	}

	emit(type: string, data: Record<string, unknown>): void {
		const event = { ...this.identity, sequence: ++this.sequence, type, ...data } as LiveEvent;
		for (const listener of [...this.listeners]) listener(event);
	}

	close(): void {}
}

function enableNativeInputSurface(host: ArtifactHost): void {
	const originalSnapshot = host.snapshot.bind(host);
	host.snapshot = () => ({
		...originalSnapshot(),
		inputImageCapability: {
			protocol: "pi.input-images", version: 1, messageBlock: "input_image_reference", metadataQuery: "input_image_history",
			maxImageBytes: 33554432, copyChunkBytes: 262144, maxPendingImages: 256,
			maxPendingDeclaredBytes: 268435456, maxPendingMetadataBytes: 262144, binaryInLive: false,
		},
		inputImages: [],
		inputImageCoverage: {
			scope: "current_branch_input_images", leafId: null, startIndex: 0, endIndexExclusive: 0,
			totalInputImages: 0, byteLength: 2, hasOlder: false, hasNewer: false,
		},
		pendingInputImages: [],
	} as LiveSnapshot);
	Object.assign(host, {
		getCurrentBranchInputImage: () => { throw Object.assign(new Error("missing"), { code: "input_image_not_found" }); },
		copyInputImageBytes: async () => ({ byteLength: 0, sha256: "0".repeat(64) }),
	});
}

function targetRecord(identity: LiveIdentity) {
	return { stream_id: identity.streamId, session_id: identity.sessionId, session_epoch: identity.sessionEpoch };
}

function runtimeEnvelope(mapping: ManagedArtifactRuntimeMapping, schema: 55 | 56 = 55) {
	return { schema, protocol: "sesh.pi-artifact-map" as const, version: 1 as const, mapping };
}

function stagingMapping(
	request: ManagedArtifactResolveRequest,
	producer = request.consumer_target,
): Extract<ManagedArtifactRuntimeMapping, { status: "staging" }> {
	return {
		mapping_id: deriveManagedArtifactClientId({
			threadId: request.thread_id,
			nativeSessionId: request.native_session_id,
			publicationId: request.publication.publication_id,
		}),
		publication_id: request.publication.publication_id,
		status: "staging",
		producer_target: producer,
		artifact_id: randomUUID(),
		updated_at_unix_ms: 1,
	};
}

function readyMapping(
	request: ManagedArtifactResolveRequest,
	producer: ReturnType<typeof targetRecord>,
	artifactId: string,
): Extract<ManagedArtifactRuntimeMapping, { status: "ready" }> {
	return {
		mapping_id: deriveManagedArtifactClientId({
			threadId: request.thread_id,
			nativeSessionId: request.native_session_id,
			publicationId: request.publication.publication_id,
		}),
		publication_id: request.publication.publication_id,
		status: "ready",
		producer_target: producer,
		artifact: {
			id: artifactId,
			thread_id: request.thread_id,
			target: producer,
			name: request.publication.name,
			media_type: request.publication.media_type,
			size: request.publication.size,
			sha256: request.publication.sha256!,
			disposition: request.publication.disposition,
			expires_at_unix_ms: Date.now() + 60_000,
		},
		updated_at_unix_ms: 2,
	};
}

async function readBody(request: http.IncomingMessage): Promise<Buffer> {
	const chunks: Buffer[] = [];
	for await (const chunk of request) chunks.push(Buffer.from(chunk));
	return Buffer.concat(chunks);
}

async function unixHttpServer(
	t: TestContext,
	handler: http.RequestListener,
): Promise<{ socketPath: string; server: http.Server }> {
	const root = await fs.promises.mkdtemp(path.join(os.tmpdir(), "pi-managed-artifacts-"));
	const socketPath = path.join(root, "sesh.sock");
	const server = http.createServer(handler);
	await new Promise<void>((resolve, reject) => {
		server.once("error", reject);
		server.listen(socketPath, resolve);
	});
	t.after(async () => {
		await new Promise<void>((resolve) => server.close(() => resolve()));
		await fs.promises.rm(root, { recursive: true, force: true });
	});
	return { socketPath, server };
}

async function liveRequest(socketPath: string, request: unknown): Promise<any> {
	const socket = net.createConnection(socketPath);
	await new Promise<void>((resolve, reject) => {
		socket.once("connect", resolve);
		socket.once("error", reject);
	});
	return await new Promise((resolve, reject) => {
		let buffer = "";
		socket.setEncoding("utf8");
		socket.on("data", (chunk) => {
			buffer += chunk;
			const newline = buffer.indexOf("\n");
			if (newline !== -1) {
				const value = JSON.parse(buffer.slice(0, newline));
				socket.destroy();
				resolve(value);
			}
		});
		socket.on("error", reject);
		socket.write(JSON.stringify(request) + "\n");
	});
}

test("managed mapping identity uses the published deterministic UUIDv8 derivation", () => {
	assert.equal(deriveManagedArtifactClientId({
		threadId: THREAD_ID,
		nativeSessionId: SESSION_ID,
		publicationId: PUBLICATION_ID,
	}), "d1b6d7ef-8eb2-82c8-8b55-b7a3b34a7e3d");
});

test("publisher resolves and backpressures exact Unix content without inventing live events", async (t) => {
	const bytes = Buffer.alloc(700_000, 0x5a);
	const artifact = projection(PUBLICATION_ID, bytes);
	const identity = { streamId: "stream-native-proof", sessionId: SESSION_ID, sessionEpoch: 4 };
	const host = new ArtifactHost(identity, [artifact]);
	host.bytes.set(PUBLICATION_ID, bytes);
	const requests: Array<{ method?: string; url?: string; headers: http.IncomingHttpHeaders; body: Buffer }> = [];
	let producer: ReturnType<typeof targetRecord> | undefined;
	const artifactId = randomUUID();
	let ready = false;
	const { socketPath } = await unixHttpServer(t, async (request, response) => {
		const body = await readBody(request);
		requests.push({ method: request.method, url: request.url, headers: request.headers, body });
		if (request.method === "POST" && request.url === "/v1/runtime/artifact-mappings/resolve") {
			const resolve = JSON.parse(body.toString("utf8")) as ManagedArtifactResolveRequest;
			producer ??= resolve.consumer_target;
			response.setHeader("Content-Type", "application/json");
			response.end(JSON.stringify(runtimeEnvelope(ready
				? readyMapping(resolve, producer, artifactId)
				: { ...stagingMapping(resolve, producer), artifact_id: artifactId })));
			return;
		}
		if (request.method === "PUT" && request.url?.startsWith("/v1/runtime/artifact-mappings/content?")) {
			assert.deepEqual(body, bytes);
			const resolve = requests[0] ? JSON.parse(requests[0].body.toString("utf8")) as ManagedArtifactResolveRequest : undefined;
			assert.ok(resolve && producer);
			ready = true;
			response.setHeader("Content-Type", "application/json");
			response.end(JSON.stringify(runtimeEnvelope(readyMapping(resolve, producer, artifactId))));
			return;
		}
		response.statusCode = 404;
		response.end(JSON.stringify({ schema: 55, status: "error", error: { code: "mapping_not_found", message: "not found" } }));
	});

	const publisher = await startManagedArtifactPublisher({ host, threadId: THREAD_ID, apiSocketPath: socketPath });
	t.after(() => publisher.close());
	await waitFor(() => ready);
	assert.equal(requests[0]!.method, "POST");
	assert.equal(requests[0]!.url, "/v1/runtime/artifact-mappings/resolve");
	assert.deepEqual(JSON.parse(requests[0]!.body.toString("utf8")), {
		thread_id: THREAD_ID,
		consumer_target: targetRecord(identity),
		native_session_id: SESSION_ID,
		publication: {
			publication_id: PUBLICATION_ID,
			kind: "image",
			name: "output.png",
			media_type: "image/png",
			size: bytes.length,
			sha256: artifact.sha256,
			disposition: "inline",
			retention: "runtime_source",
			origin: "native",
			availability: { status: "available" },
		},
	});
	const content = requests.find((entry) => entry.method === "PUT")!;
	assert.match(content.url!, /^\/v1\/runtime\/artifact-mappings\/content\?thread_id=/);
	assert.equal(content.headers["content-type"], "application/octet-stream");
	assert.equal(content.headers["content-length"], String(bytes.length));
	assert.equal(content.headers["x-sesh-consumer-stream-id"], identity.streamId);
	assert.equal(content.headers["x-sesh-consumer-session-id"], SESSION_ID);
	assert.equal(content.headers["x-sesh-consumer-session-epoch"], String(identity.sessionEpoch));
	assert.equal(host.copyCalls.length, 1);
	assert.deepEqual(host.copyCalls[0]!.options, {
		maxBytes: 32 * 1024 * 1024,
		chunkBytes: 256 * 1024,
		timeoutMs: 10_000,
		signal: host.copyCalls[0]!.options.signal,
	});
	assert.equal(host.sequence, 0);
	const requestCount = requests.length;
	host.emit("input_image_referenced", {
		inputImage: {
			inputImageId: randomUUID(), mediaType: "image/png", byteLength: 3, sha256: "a".repeat(64),
			source: { type: "user_message_image", contentIndex: 0 }, origin: "native", availability: { status: "available" },
		},
	});
	await delay(20);
	assert.equal(requests.length, requestCount, "input images must not create output mappings");
});

test("combined text leases and output publication have one exact marker writer and truthful capabilities", async (t) => {
	const root = await fs.promises.mkdtemp(path.join(os.tmpdir(), "pi-managed-marker-"));
	const inbox = path.join(root, "inbox");
	await fs.promises.mkdir(path.join(inbox, "leases"), { recursive: true, mode: 0o700 });
	await fs.promises.chmod(inbox, 0o700);
	await fs.promises.chmod(path.join(inbox, "leases"), 0o700);
	const socketPath = path.join(root, "live.sock");
	const apiSocketPath = path.join(root, "sesh.sock");
	const host = new ArtifactHost({ streamId: "marker-stream", sessionId: SESSION_ID, sessionEpoch: 0 });
	enableNativeInputSurface(host);
	const unusedClient: ManagedArtifactSeshClient = {
		async resolveMapping() { throw new Error("no publication expected"); },
		async putMappingContent() { throw new Error("no publication expected"); },
		async failMapping() { throw new Error("no publication expected"); },
	};
	const server = await startLiveTransport(host, {
		socketPath,
		attachments: {
			threadId: THREAD_ID,
			inboxPath: inbox,
			apiSocketPath,
			prepareRpcPromptFiles: async ({ command }) => command,
		},
		managedArtifacts: { threadId: THREAD_ID, inboxPath: inbox, apiSocketPath, client: unusedClient },
		inputImageReader: { threadId: THREAD_ID, inboxPath: inbox, apiSocketPath },
	});
	t.after(async () => {
		await server.close();
		await fs.promises.rm(root, { recursive: true, force: true });
	});
	const markerPath = path.join(inbox, "consumer.json");
	const marker = JSON.parse(await fs.promises.readFile(markerPath, "utf8"));
	assert.match(marker.input_image_reader.listener_generation, /^[0-9a-f-]{36}$/);
	assert.deepEqual(marker, {
		protocol: "sesh.pi-transfer",
		version: 1,
		thread_id: THREAD_ID,
		target: targetRecord(host.identity),
		capabilities: ["attachment-lease-v1", "artifact-publish-v1", "input-image-read-v1"],
		input_image_reader: {
			protocol: "pi.input-image-read",
			version: 1,
			listener_generation: marker.input_image_reader.listener_generation,
			socket_suffix: ".input.sock",
		},
	});
	assert.deepEqual((await fs.promises.readdir(inbox)).filter((name) => name.startsWith(".consumer-")), []);
	const capabilities = await liveRequest(socketPath, { protocol: "live-v1", id: "caps", op: "capabilities" });
	assert.equal(capabilities.result.attachments.imageInput, false);
	assert.deepEqual(capabilities.result.managedArtifacts, MANAGED_ARTIFACT_CAPABILITY);
	assert.deepEqual(capabilities.result.inputImageBytes, LIVE_INPUT_IMAGE_READER_CAPABILITY);
	const inputSocketPath = path.join(root, `${SESSION_ID}.input.sock`);
	assert.equal((await fs.promises.stat(inputSocketPath)).mode & 0o777, 0o600);
	await server.close();
	assert.equal(fs.existsSync(markerPath), false);
	assert.equal(fs.existsSync(inputSocketPath), false);
});

test("input reader startup failure preserves text attachments and managed output", async (t) => {
	const root = await fs.promises.mkdtemp(path.join(os.tmpdir(), "pi-input-reader-additive-"));
	const inbox = path.join(root, "inbox");
	await fs.promises.mkdir(path.join(inbox, "leases"), { recursive: true, mode: 0o700 });
	await fs.promises.chmod(inbox, 0o700);
	await fs.promises.chmod(path.join(inbox, "leases"), 0o700);
	const liveSocket = path.join(root, "live.sock");
	const inputSocket = path.join(root, `${SESSION_ID}.input.sock`);
	const apiSocketPath = path.join(root, "sesh.sock");
	const blocker = net.createServer((socket) => socket.destroy());
	await new Promise<void>((resolve, reject) => {
		blocker.once("error", reject);
		blocker.listen(inputSocket, resolve);
	});
	const host = new ArtifactHost({ streamId: "input-additive", sessionId: SESSION_ID, sessionEpoch: 0 });
	enableNativeInputSurface(host);
	const reports: unknown[] = [];
	const unusedClient: ManagedArtifactSeshClient = {
		async resolveMapping() { throw new Error("no publication expected"); },
		async putMappingContent() { throw new Error("no publication expected"); },
		async failMapping() { throw new Error("no publication expected"); },
	};
	const server = await startLiveTransport(host, {
		socketPath: liveSocket,
		inputImageSocketPath: inputSocket,
		onError: (error) => reports.push(error),
		attachments: {
			threadId: THREAD_ID, inboxPath: inbox, apiSocketPath,
			prepareRpcPromptFiles: async ({ command }) => command,
		},
		managedArtifacts: { threadId: THREAD_ID, inboxPath: inbox, apiSocketPath, client: unusedClient },
		inputImageReader: { threadId: THREAD_ID, inboxPath: inbox, apiSocketPath },
	});
	t.after(async () => {
		await server.close().catch(() => undefined);
		await new Promise<void>((resolve) => blocker.close(() => resolve()));
		await fs.promises.rm(root, { recursive: true, force: true });
	});
	assert.ok(reports.length >= 1);
	const marker = JSON.parse(await fs.promises.readFile(path.join(inbox, "consumer.json"), "utf8"));
	assert.deepEqual(marker.capabilities, ["attachment-lease-v1", "artifact-publish-v1"]);
	assert.equal(marker.input_image_reader, undefined);
	const capabilities = await liveRequest(liveSocket, { protocol: "live-v1", id: "additive-caps", op: "capabilities" });
	assert.equal(capabilities.result.attachments.imageInput, false);
	assert.deepEqual(capabilities.result.managedArtifacts, MANAGED_ARTIFACT_CAPABILITY);
	assert.ok(capabilities.result.inputImages);
	assert.equal(capabilities.result.inputImageBytes, undefined);
	assert.equal((await fs.promises.lstat(inputSocket)).isSocket(), true);
});

test("terminal copy failures use the narrow fail union while retryable failures stay pending", async (t) => {
	const digestArtifact = projection(randomUUID(), Buffer.from("digest"));
	const timeoutArtifact = projection(randomUUID(), Buffer.from("timeout"));
	const host = new ArtifactHost({ streamId: "failure-stream", sessionId: SESSION_ID, sessionEpoch: 1 }, [digestArtifact, timeoutArtifact]);
	host.copyImpl = async (_target, publicationId) => {
		if (publicationId === digestArtifact.publicationId) throw Object.assign(new Error("secret path /tmp/no"), { code: "digest_mismatch" });
		throw Object.assign(new Error("transport timeout"), { code: "timeout" });
	};
	const requests = new Map<string, ManagedArtifactResolveRequest>();
	const failCalls: any[] = [];
	const reports: string[] = [];
	const client: ManagedArtifactSeshClient = {
		async resolveMapping(request) {
			requests.set(request.publication.publication_id, request);
			return runtimeEnvelope(stagingMapping(request));
		},
		async putMappingContent(input, copy) {
			const request = [...requests.values()].find((entry) => deriveManagedArtifactClientId({
				threadId: entry.thread_id,
				nativeSessionId: entry.native_session_id,
				publicationId: entry.publication.publication_id,
			}) === input.mappingId)!;
			const copyResult = await copy(async () => undefined);
			return { envelope: runtimeEnvelope(readyMapping(request, request.consumer_target, randomUUID())), copyResult };
		},
		async failMapping(request) {
			failCalls.push(request);
			const resolved = requests.get(request.publication_id)!;
			return runtimeEnvelope({
				mapping_id: deriveManagedArtifactClientId({ threadId: resolved.thread_id, nativeSessionId: resolved.native_session_id, publicationId: request.publication_id }),
				publication_id: request.publication_id,
				status: "unavailable",
				unavailable: { source: request.failure.source, code: request.failure.code, message: request.failure.message },
				updated_at_unix_ms: 3,
			});
		},
	};
	const publisher = await startManagedArtifactPublisher({
		host,
		threadId: THREAD_ID,
		apiSocketPath: "/tmp/unused-sesh.sock",
		client,
		onError: (error) => reports.push(String(error)),
	});
	t.after(() => publisher.close());
	await waitFor(() => requests.size === 2 && reports.length >= 1 && failCalls.length === 1);
	assert.equal(failCalls[0].publication_id, digestArtifact.publicationId);
	assert.deepEqual(failCalls[0].failure, {
		source: "native_copy",
		code: "digest_mismatch",
		message: "Artifact digest did not match immutable metadata",
	});
	assert.ok(reports.every((entry) => !entry.includes("/tmp/no")));
	host.emit("artifact_published", { artifact: timeoutArtifact });
	await delay(30);
	assert.equal(requests.size, 2, "retryable work is not looped or replayed in one activation");
	assert.equal(failCalls.length, 1);
});

test("staging publication resumes across stream rotation without rewriting the producer target", async (t) => {
	const bytes = Buffer.from("rotated target bytes");
	const artifact = projection(randomUUID(), bytes);
	const firstIdentity = { streamId: "stream-before", sessionId: SESSION_ID, sessionEpoch: 2 };
	const secondIdentity = { streamId: "stream-after", sessionId: SESSION_ID, sessionEpoch: 3 };
	let producer: ReturnType<typeof targetRecord> | undefined;
	let artifactId: string | undefined;
	let ready = false;
	const resolveTargets: ReturnType<typeof targetRecord>[] = [];
	const contentTargets: ReturnType<typeof targetRecord>[] = [];
	const client: ManagedArtifactSeshClient = {
		async resolveMapping(request) {
			resolveTargets.push(request.consumer_target);
			producer ??= request.consumer_target;
			artifactId ??= randomUUID();
			return runtimeEnvelope(ready
				? readyMapping(request, producer, artifactId)
				: { ...stagingMapping(request, producer), artifact_id: artifactId });
		},
		async putMappingContent(input, copy) {
			contentTargets.push(input.consumerTarget);
			const request: ManagedArtifactResolveRequest = {
				thread_id: THREAD_ID,
				consumer_target: input.consumerTarget,
				native_session_id: SESSION_ID,
				publication: {
					publication_id: artifact.publicationId,
					kind: artifact.kind,
					name: artifact.displayName,
					media_type: artifact.mediaType,
					size: artifact.byteLength,
					sha256: artifact.sha256!,
					disposition: artifact.disposition,
					retention: artifact.retention,
					origin: artifact.origin,
					availability: { status: "available" },
				},
			};
			const copyResult = await copy(async () => undefined);
			ready = true;
			return { envelope: runtimeEnvelope(readyMapping(request, producer!, artifactId!)), copyResult };
		},
		async failMapping() { throw new Error("retryable abort must not become terminal"); },
	};
	const firstHost = new ArtifactHost(firstIdentity, [artifact]);
	firstHost.copyImpl = async () => { throw Object.assign(new Error("lifecycle ended"), { code: "aborted" }); };
	const first = await startManagedArtifactPublisher({ host: firstHost, threadId: THREAD_ID, apiSocketPath: "/tmp/unused.sock", client });
	await waitFor(() => contentTargets.length === 1);
	await first.close();
	assert.equal(ready, false);

	const secondHost = new ArtifactHost(secondIdentity, [artifact]);
	secondHost.bytes.set(artifact.publicationId, bytes);
	const second = await startManagedArtifactPublisher({ host: secondHost, threadId: THREAD_ID, apiSocketPath: "/tmp/unused.sock", client });
	await waitFor(() => ready);
	assert.deepEqual(producer, targetRecord(firstIdentity));
	assert.deepEqual(resolveTargets, [targetRecord(firstIdentity), targetRecord(secondIdentity)]);
	assert.deepEqual(contentTargets, [targetRecord(firstIdentity), targetRecord(secondIdentity)]);
	assert.equal(secondHost.copyCalls.length, 1);
	await second.close();

	const recoveredIdentity = { streamId: "stream-recovered", sessionId: SESSION_ID, sessionEpoch: 4 };
	const recoveredHost = new ArtifactHost(recoveredIdentity, [artifact]);
	const recovered = await startManagedArtifactPublisher({ host: recoveredHost, threadId: THREAD_ID, apiSocketPath: "/tmp/unused.sock", client });
	t.after(() => recovered.close());
	await waitFor(() => resolveTargets.length === 3);
	await delay(20);
	assert.deepEqual(resolveTargets[2], targetRecord(recoveredIdentity));
	assert.equal(contentTargets.length, 2, "ready crash recovery must not reopen or replace managed bytes");
	assert.equal(recoveredHost.copyCalls.length, 0);
});

test("legacy missing digests and native unavailability resolve to tombstones without opening bytes", async (t) => {
	const legacy = projection(randomUUID(), Buffer.from("legacy"), { sha256: undefined, origin: "legacy_inline_image", retention: "session_inline" });
	const unavailable = projection(randomUUID(), Buffer.from("gone"), {
		availability: { status: "unavailable", reason: "source_not_retained", message: "Runtime source is no longer retained" },
	});
	const host = new ArtifactHost({ streamId: "unavailable-stream", sessionId: SESSION_ID, sessionEpoch: 0 }, [legacy, unavailable]);
	const resolved: ManagedArtifactResolveRequest[] = [];
	const client: ManagedArtifactSeshClient = {
		async resolveMapping(request) {
			resolved.push(request);
			const nativeUnavailable = request.publication.availability.status === "unavailable"
				? {
					source: "native" as const,
					code: request.publication.availability.reason,
					message: request.publication.availability.message,
				}
				: { source: "sesh" as const, code: "digest_unavailable", message: "Authoritative native digest is unavailable" };
			return runtimeEnvelope({
				mapping_id: deriveManagedArtifactClientId({ threadId: request.thread_id, nativeSessionId: request.native_session_id, publicationId: request.publication.publication_id }),
				publication_id: request.publication.publication_id,
				status: "unavailable",
				unavailable: nativeUnavailable,
				updated_at_unix_ms: 1,
			});
		},
		async putMappingContent() { throw new Error("unavailable mappings must not open content"); },
		async failMapping() { throw new Error("resolve owns these tombstones"); },
	};
	const publisher = await startManagedArtifactPublisher({ host, threadId: THREAD_ID, apiSocketPath: "/tmp/unused.sock", client });
	t.after(() => publisher.close());
	await waitFor(() => resolved.length === 2);
	assert.equal(resolved.find((entry) => entry.publication.publication_id === legacy.publicationId)!.publication.sha256, null);
	assert.deepEqual(resolved.find((entry) => entry.publication.publication_id === unavailable.publicationId)!.publication.availability, {
		status: "unavailable",
		reason: "source_not_retained",
		message: "Runtime source is no longer retained",
	});
	assert.equal(host.copyCalls.length, 0);
});

test("publisher bounds copy concurrency and reconciles bounded queue overflow without duplicate attempts", async (t) => {
	const artifacts = Array.from({ length: 10 }, (_, index) => {
		const bytes = Buffer.alloc(64 * 1024 + index, index);
		return { artifact: projection(randomUUID(), bytes), bytes };
	});
	const host = new ArtifactHost({ streamId: "bounded-stream", sessionId: SESSION_ID, sessionEpoch: 0 }, artifacts.map((entry) => entry.artifact));
	for (const entry of artifacts) host.bytes.set(entry.artifact.publicationId, entry.bytes);
	const requests = new Map<string, ManagedArtifactResolveRequest>();
	let activeCopies = 0;
	let maxActiveCopies = 0;
	let readyCount = 0;
	const resolveCounts = new Map<string, number>();
	const client: ManagedArtifactSeshClient = {
		async resolveMapping(request) {
			requests.set(request.publication.publication_id, request);
			resolveCounts.set(request.publication.publication_id, (resolveCounts.get(request.publication.publication_id) ?? 0) + 1);
			return runtimeEnvelope(stagingMapping(request));
		},
		async putMappingContent(input, copy) {
			activeCopies++;
			maxActiveCopies = Math.max(maxActiveCopies, activeCopies);
			try {
				await delay(15);
				const copyResult = await copy(async () => undefined);
				const request = [...requests.values()].find((entry) => deriveManagedArtifactClientId({
					threadId: entry.thread_id,
					nativeSessionId: entry.native_session_id,
					publicationId: entry.publication.publication_id,
				}) === input.mappingId)!;
				readyCount++;
				return { envelope: runtimeEnvelope(readyMapping(request, request.consumer_target, randomUUID())), copyResult };
			} finally {
				activeCopies--;
			}
		},
		async failMapping() { throw new Error("bounded success fixture must not fail"); },
	};
	const publisher = await startManagedArtifactPublisher({
		host,
		threadId: THREAD_ID,
		apiSocketPath: "/tmp/unused.sock",
		client,
		limits: { maxConcurrentCopies: 2, maxQueuedPublications: 4, maxObservedMappings: 20 },
	});
	t.after(() => publisher.close());
	await waitFor(() => readyCount === artifacts.length, 5000);
	assert.equal(maxActiveCopies, 2);
	assert.equal(resolveCounts.size, artifacts.length);
	assert.ok([...resolveCounts.values()].every((count) => count === 1));
});

test("runtime client accepts exactly schema 55 and 56 success and error envelopes", async (t) => {
	const request: ManagedArtifactResolveRequest = {
		thread_id: THREAD_ID,
		consumer_target: { stream_id: "schema-transition", session_id: SESSION_ID, session_epoch: 0 },
		native_session_id: SESSION_ID,
		publication: {
			publication_id: PUBLICATION_ID,
			kind: "file",
			name: "file.bin",
			media_type: "application/octet-stream",
			size: 0,
			sha256: createHash("sha256").update(Buffer.alloc(0)).digest("hex"),
			disposition: "attachment",
			retention: "runtime_source",
			origin: "native",
			availability: { status: "available" },
		},
	};
	for (const schema of [55, 56] as const) {
		const success = await unixHttpServer(t, async (incoming, response) => {
			await readBody(incoming);
			response.setHeader("Content-Type", "application/json");
			response.end(JSON.stringify(runtimeEnvelope(stagingMapping(request), schema)));
		});
		const successClient = createManagedArtifactSeshClient(success.socketPath);
		assert.equal((await successClient.resolveMapping(request, new AbortController().signal)).schema, schema);

		const failure = await unixHttpServer(t, async (incoming, response) => {
			await readBody(incoming);
			response.statusCode = 409;
			response.setHeader("Content-Type", "application/json");
			response.end(JSON.stringify({ schema, status: "error", error: { code: `schema_${schema}_error`, message: "exact error" } }));
		});
		const failureClient = createManagedArtifactSeshClient(failure.socketPath);
		await assert.rejects(
			failureClient.resolveMapping(request, new AbortController().signal),
			(error: unknown) => error instanceof Error
				&& (error as Error & { code?: string }).code === `schema_${schema}_error`
				&& error.message === "exact error",
		);
	}
});

test("runtime client rejects internal pending and unknown response fields", async (t) => {
	const { socketPath } = await unixHttpServer(t, async (request, response) => {
		await readBody(request);
		response.setHeader("Content-Type", "application/json");
		response.end(JSON.stringify({
			schema: 55,
			protocol: "sesh.pi-artifact-map",
			version: 1,
			mapping: {
				mapping_id: randomUUID(),
				publication_id: PUBLICATION_ID,
				status: "pending",
				updated_at_unix_ms: 1,
			},
			extra: "must be refused",
		}));
	});
	const client = createManagedArtifactSeshClient(socketPath);
	const abort = new AbortController();
	await assert.rejects(client.resolveMapping({
		thread_id: THREAD_ID,
		consumer_target: { stream_id: "strict", session_id: SESSION_ID, session_epoch: 0 },
		native_session_id: SESSION_ID,
		publication: {
			publication_id: PUBLICATION_ID,
			kind: "file",
			name: "file.bin",
			media_type: "application/octet-stream",
			size: 0,
			sha256: createHash("sha256").update(Buffer.alloc(0)).digest("hex"),
			disposition: "attachment",
			retention: "runtime_source",
			origin: "native",
			availability: { status: "available" },
		},
	}, abort.signal), /response envelope is invalid/);
});

test("runtime client enforces an overall Sesh request deadline", async (t) => {
	const { socketPath } = await unixHttpServer(t, async (request, response) => {
		await readBody(request);
		await delay(100);
		if (!response.destroyed) response.end();
	});
	const client = createManagedArtifactSeshClient(socketPath, { seshRequestTimeoutMs: 20, maxResponseBytes: 64 * 1024 });
	const abort = new AbortController();
	await assert.rejects(client.resolveMapping({
		thread_id: THREAD_ID,
		consumer_target: { stream_id: "deadline", session_id: SESSION_ID, session_epoch: 0 },
		native_session_id: SESSION_ID,
		publication: {
			publication_id: PUBLICATION_ID,
			kind: "file",
			name: "file.bin",
			media_type: "application/octet-stream",
			size: 0,
			sha256: createHash("sha256").update(Buffer.alloc(0)).digest("hex"),
			disposition: "attachment",
			retention: "runtime_source",
			origin: "native",
			availability: { status: "available" },
		},
	}, abort.signal), /timed out/);
});
