import { test, type TestContext } from "node:test";
import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as net from "node:net";
import * as os from "node:os";
import * as path from "node:path";
import { createHash, randomUUID } from "node:crypto";
import {
	ATTACHMENT_CAPABILITY,
	readLiveAttachmentEnvironment,
	type PrepareRpcPromptFiles,
} from "../attachment-transfer.ts";
import {
	startLiveTransport,
	type LiveEvent,
	type LiveIdentity,
	type LiveSnapshot,
	type LiveTransportHost,
} from "../live-transport.ts";

class AttachmentHost implements LiveTransportHost {
	readonly identity: LiveIdentity = { streamId: "stream-attachments", sessionEpoch: 2, sessionId: "session-attachments" };
	sequence = 0;
	dispatchCalls: Array<{ target: LiveIdentity; command: unknown }> = [];
	snapshotExtra: Record<string, unknown> = {};
	dispatchImpl: (target: LiveIdentity, command: unknown) => Promise<unknown> = async (_target, command) => ({
		type: (command as { type: string }).type,
		success: true,
	});
	private listeners = new Set<(event: LiveEvent) => void>();

	snapshot(): LiveSnapshot {
		return { ...this.identity, sequence: this.sequence, ...this.snapshotExtra };
	}

	subscribe(listener: (event: LiveEvent) => void): () => void {
		this.listeners.add(listener);
		return () => this.listeners.delete(listener);
	}

	async dispatch(target: LiveIdentity, command: unknown): Promise<unknown> {
		this.dispatchCalls.push({ target, command });
		return await this.dispatchImpl(target, command);
	}

	emit(type: string, data: Record<string, unknown>): LiveEvent {
		const event = { ...this.identity, sequence: ++this.sequence, type, ...data } as LiveEvent;
		for (const listener of [...this.listeners]) listener(event);
		return event;
	}

	close(): void {
		this.listeners.clear();
	}
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

type TransferFixture = {
	root: string;
	inbox: string;
	socketPath: string;
	apiSocketPath: string;
	threadId: string;
	host: AttachmentHost;
};

async function transferFixture(t: TestContext): Promise<TransferFixture> {
	const root = await fs.promises.mkdtemp(path.join(os.tmpdir(), "pi-live-attachments-"));
	const inbox = path.join(root, "inbox");
	const leases = path.join(inbox, "leases");
	await fs.promises.mkdir(leases, { recursive: true, mode: 0o700 });
	await fs.promises.chmod(inbox, 0o700);
	await fs.promises.chmod(leases, 0o700);
	const socketPath = path.join(root, "fixture.live.sock");
	const apiSocketPath = path.join(root, "sesh.sock");
	const threadId = randomUUID();
	const host = new AttachmentHost();
	t.after(async () => fs.promises.rm(root, { recursive: true, force: true }));
	return { root, inbox, socketPath, apiSocketPath, threadId, host };
}

async function writeLease(
	fixture: TransferFixture,
	input: {
		leaseId?: string;
		requestId: string;
		name?: string;
		bytes?: Buffer;
		target?: { stream_id: string; session_id: string; session_epoch: number };
		threadId?: string;
		expiresAt?: number;
		sha256?: string;
		declaredSize?: number;
		payloadSymlink?: boolean;
	},
): Promise<string> {
	const leaseId = input.leaseId ?? randomUUID();
	const bytes = input.bytes ?? Buffer.from("attachment bytes");
	const directory = path.join(fixture.inbox, "leases", leaseId);
	await fs.promises.mkdir(directory, { mode: 0o700 });
	await fs.promises.chmod(directory, 0o700);
	const payload = path.join(directory, "payload");
	if (input.payloadSymlink) {
		const elsewhere = path.join(fixture.root, `outside-${leaseId}`);
		await fs.promises.writeFile(elsewhere, bytes, { mode: 0o600 });
		await fs.promises.symlink(elsewhere, payload);
	} else {
		await fs.promises.writeFile(payload, bytes, { mode: 0o600 });
		await fs.promises.chmod(payload, 0o600);
	}
	const lease = {
		protocol: "sesh.pi-transfer",
		version: 1,
		type: "attachment-lease",
		lease_id: leaseId,
		thread_id: input.threadId ?? fixture.threadId,
		attachment_id: randomUUID(),
		target: input.target ?? {
			stream_id: fixture.host.identity.streamId,
			session_id: fixture.host.identity.sessionId,
			session_epoch: fixture.host.identity.sessionEpoch,
		},
		request_id: input.requestId,
		name: input.name ?? "attachment.txt",
		media_type: "text/plain",
		size: input.declaredSize ?? bytes.length,
		sha256: input.sha256 ?? createHash("sha256").update(bytes).digest("hex"),
		payload: "payload",
		expires_at_unix_ms: input.expiresAt ?? Date.now() + 60_000,
	};
	const metadata = path.join(directory, "lease.json");
	await fs.promises.writeFile(metadata, JSON.stringify(lease), { mode: 0o600 });
	await fs.promises.chmod(metadata, 0o600);
	return leaseId;
}

async function startTransferServer(
	fixture: TransferFixture,
	preparer: PrepareRpcPromptFiles,
	onError: (error: unknown) => void = () => undefined,
) {
	return await startLiveTransport(fixture.host, {
		socketPath: fixture.socketPath,
		onError,
		attachments: {
			threadId: fixture.threadId,
			inboxPath: fixture.inbox,
			apiSocketPath: fixture.apiSocketPath,
			prepareRpcPromptFiles: preparer,
		},
	});
}

test("the Sesh transfer environment is all-or-none and canonical", () => {
	assert.deepEqual(readLiveAttachmentEnvironment({}), { status: "absent" });
	assert.equal(readLiveAttachmentEnvironment({ SESH_PI_TRANSFER_THREAD_ID: randomUUID() }).status, "invalid");
	assert.equal(readLiveAttachmentEnvironment({
		SESH_PI_TRANSFER_THREAD_ID: "AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA",
		SESH_PI_TRANSFER_INBOX: "/tmp/inbox",
		SESH_PI_TRANSFER_API_SOCKET: "/tmp/sesh.sock",
	}).status, "invalid");
	const threadId = randomUUID();
	assert.deepEqual(readLiveAttachmentEnvironment({
		SESH_PI_TRANSFER_THREAD_ID: threadId,
		SESH_PI_TRANSFER_INBOX: "/tmp/inbox",
		SESH_PI_TRANSFER_API_SOCKET: "/tmp/sesh.sock",
	}), { status: "present", threadId, inboxPath: "/tmp/inbox", apiSocketPath: "/tmp/sesh.sock" });
});

test("attachment capability, marker and native preparation stay out of the wire", async (t) => {
	const fixture = await transferFixture(t);
	fixture.host.snapshotExtra = {
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
	};
	Object.assign(fixture.host, {
		getCurrentBranchInputImage: () => { throw Object.assign(new Error("missing"), { code: "input_image_not_found" }); },
		copyInputImageBytes: async () => ({ byteLength: 0, sha256: "0".repeat(64) }),
	});
	const requestId = randomUUID();
	const secret = Buffer.from("SECRET-ATTACHMENT-CONTENT");
	const secondSecret = Buffer.from("SECOND-ORDERED-CONTENT");
	const leaseId = await writeLease(fixture, { requestId, bytes: secret, name: "notes.txt" });
	const secondLeaseId = await writeLease(fixture, { requestId, bytes: secondSecret, name: "second.txt" });
	let observedBytes: Buffer[] = [];
	let observedScratchPaths: string[] = [];
	const server = await startTransferServer(fixture, async (input) => {
		observedScratchPaths = input.files.map((file) => file.path);
		observedBytes = await Promise.all(input.files.map((file) => fs.promises.readFile(file.path)));
		assert.deepEqual(input.files.map((file) => file.displayName), ["notes.txt", "second.txt"]);
		assert.equal(input.maxTextBytesPerFile, 1024 * 1024);
		assert.equal(input.maxTextBytesTotal, 2 * 1024 * 1024);
		return { ...input.command, message: `<file name="notes.txt">prepared</file>\n${String(input.command.message)}` };
	});
	t.after(() => server.close());

	const markerPath = path.join(fixture.inbox, "consumer.json");
	const marker = JSON.parse(await fs.promises.readFile(markerPath, "utf8"));
	assert.deepEqual(marker, {
		protocol: "sesh.pi-transfer",
		version: 1,
		thread_id: fixture.threadId,
		target: {
			stream_id: fixture.host.identity.streamId,
			session_id: fixture.host.identity.sessionId,
			session_epoch: fixture.host.identity.sessionEpoch,
		},
		capabilities: ["attachment-lease-v1"],
	});
	assert.equal((await fs.promises.stat(markerPath)).mode & 0o777, 0o600);
	assert.deepEqual((await fs.promises.readdir(fixture.inbox)).filter((name) => name.startsWith(".consumer-")), []);

	const client = await JsonlClient.connect(fixture.socketPath);
	t.after(() => client.close());
	client.send({ protocol: "live-v1", id: "caps", op: "capabilities" });
	const capabilities = (await client.next()).result;
	assert.deepEqual(capabilities.attachments, ATTACHMENT_CAPABILITY);
	assert.equal(capabilities.attachments.imageInput, false);
	assert.equal(capabilities.inputImages.protocol, "pi.input-images");
	client.send({
		protocol: "live-v1",
		id: requestId,
		op: "dispatch",
		target: fixture.host.identity,
		command: { type: "prompt", id: "native-prompt", message: "original" },
		attachments: [{ leaseId }, { leaseId: secondLeaseId }],
	});
	const response = await client.next();
	assert.equal(response.result.status, "completed");
	assert.ok(!JSON.stringify(response).includes(secret.toString("utf8")));
	assert.ok(!JSON.stringify(response).includes(secondSecret.toString("utf8")));
	assert.deepEqual(observedBytes, [secret, secondSecret]);
	assert.equal(fixture.host.dispatchCalls.length, 1);
	assert.deepEqual(fixture.host.dispatchCalls[0]!.command, {
		type: "prompt",
		id: "native-prompt",
		message: "<file name=\"notes.txt\">prepared</file>\noriginal",
	});
	assert.equal(observedScratchPaths.length, 2);
	assert.ok(observedScratchPaths.every((scratchPath) => !fs.existsSync(scratchPath)));
});

test("lease basenames reject dot entries while valid Unicode names and the false image gate remain unchanged", async (t) => {
	const fixture = await transferFixture(t);
	const preparedNames: string[][] = [];
	const server = await startTransferServer(fixture, async (input) => {
		preparedNames.push(input.files.map((file) => file.displayName));
		return input.command;
	});
	t.after(() => server.close());
	const client = await JsonlClient.connect(fixture.socketPath);
	t.after(() => client.close());

	client.send({ protocol: "live-v1", id: "basename-caps", op: "capabilities" });
	assert.equal((await client.next()).result.attachments.imageInput, false);

	for (const name of [".", ".."] as const) {
		const requestId = randomUUID();
		const leaseId = await writeLease(fixture, { requestId, name });
		client.send({
			protocol: "live-v1",
			id: requestId,
			op: "dispatch",
			target: fixture.host.identity,
			command: { type: "prompt", message: `reject ${name}` },
			attachments: [{ leaseId }],
		});
		const failed = await client.next();
		assert.equal(failed.error.code, "lease_corrupt", name);
		client.send({
			protocol: "live-v1",
			id: `basename-result-${name === "." ? "dot" : "dot-dot"}`,
			op: "get_result",
			target: fixture.host.identity,
			requestId,
		});
		const retained = await client.next();
		assert.equal(retained.result.status, "failed", name);
		assert.equal(retained.result.error.code, "lease_corrupt", name);
	}
	assert.deepEqual(preparedNames, []);
	assert.equal(fixture.host.dispatchCalls.length, 0);

	const validRequestId = randomUUID();
	const validName = "résumé-你好.txt";
	const validLeaseId = await writeLease(fixture, { requestId: validRequestId, name: validName });
	client.send({
		protocol: "live-v1",
		id: validRequestId,
		op: "dispatch",
		target: fixture.host.identity,
		command: { type: "prompt", message: "valid basename" },
		attachments: [{ leaseId: validLeaseId }],
	});
	assert.equal((await client.next()).result.status, "completed");
	assert.deepEqual(preparedNames, [[validName]]);
	assert.equal(fixture.host.dispatchCalls.length, 1);
});

test("image-bearing leases fail before dispatch and unrelated image history remains canonical", async (t) => {
	const fixture = await transferFixture(t);
	const requestId = randomUUID();
	const leaseBytes = Buffer.from("image source bytes");
	const leaseId = await writeLease(fixture, { requestId, bytes: leaseBytes, name: "image.png" });
	const preparedImageData = Buffer.from("PREPARED-IMAGE-MUST-NOT-BE-DISPATCHED").toString("base64");
	const historicalImageData = Buffer.from("UNRELATED-HISTORICAL-IMAGE").toString("base64");
	const historicalMessage = {
		role: "user",
		content: [
			{ type: "text", text: "existing image" },
			{ type: "image", data: historicalImageData, mimeType: "image/png" },
		],
		timestamp: Date.now(),
	};
	fixture.host.snapshotExtra = { messages: [historicalMessage] };
	fixture.host.dispatchImpl = async (_target, command) => {
		if ((command as { type?: string }).type === "get_messages") return { messages: [historicalMessage] };
		throw new Error("image-bearing prompt reached native dispatch");
	};
	const server = await startTransferServer(fixture, async (input) => ({
		...input.command,
		images: [{ type: "image", data: preparedImageData, mimeType: "image/png" }],
	}));
	t.after(() => server.close());

	const current = await JsonlClient.connect(fixture.socketPath);
	t.after(() => current.close());
	current.send({ protocol: "live-v1", id: "image-caps", op: "capabilities" });
	assert.equal((await current.next()).result.attachments.imageInput, false);
	current.send({ protocol: "live-v1", id: "image-sub", op: "subscribe" });
	await current.next();
	const initialSnapshot = await current.next();
	assert.equal(initialSnapshot.snapshot.messages[0].content[1].data, historicalImageData);

	fixture.host.emit("session_event", { event: { type: "message_start", message: historicalMessage } });
	const liveHistorical = await current.next();
	assert.equal(liveHistorical.event.event.message.content[1].data, historicalImageData);

	current.send({
		protocol: "live-v1",
		id: requestId,
		op: "dispatch",
		target: fixture.host.identity,
		command: { type: "prompt", message: "inspect image" },
		attachments: [{ leaseId }],
	});
	const failed = await current.next();
	assert.equal(failed.ok, false);
	assert.equal(failed.error.code, "image_input_unsupported");
	assert.match(failed.error.message, /no prompt was dispatched/);
	assert.ok(!JSON.stringify(failed).includes(preparedImageData));
	assert.equal(fixture.host.dispatchCalls.length, 0);
	assert.deepEqual(
		await fs.promises.readFile(path.join(fixture.inbox, "leases", leaseId, "payload")),
		leaseBytes,
	);

	current.send({ protocol: "live-v1", id: "image-result", op: "get_result", target: fixture.host.identity, requestId });
	const retained = await current.next();
	assert.equal(retained.result.status, "failed");
	assert.equal(retained.result.error.code, "image_input_unsupported");
	current.send({
		protocol: "live-v1",
		id: requestId,
		op: "dispatch",
		target: fixture.host.identity,
		command: { type: "prompt", message: "must not retry image input" },
		attachments: [{ leaseId }],
	});
	assert.equal((await current.next()).error.code, "duplicate_request");

	current.send({
		protocol: "live-v1",
		id: "image-history",
		op: "query",
		target: fixture.host.identity,
		command: { type: "get_messages" },
	});
	const history = await current.next();
	assert.equal(history.result.messages[0].content[1].data, historicalImageData);
	assert.equal(fixture.host.dispatchCalls.length, 1);
	assert.equal((fixture.host.dispatchCalls[0]!.command as { type: string }).type, "get_messages");

	const replay = await JsonlClient.connect(fixture.socketPath);
	t.after(() => replay.close());
	replay.send({
		protocol: "live-v1",
		id: "image-replay",
		op: "subscribe",
		after: { streamId: fixture.host.identity.streamId, sequence: 0 },
	});
	assert.equal((await replay.next()).result.mode, "replay");
	const replayed = await replay.next();
	assert.equal(replayed.event.event.message.content[1].data, historicalImageData);
	assert.ok(fixture.host.dispatchCalls.every(({ command }) => (command as { type?: string }).type !== "prompt"));
});

test("lease failures are retained before Pi dispatch and never expose managed paths", async (t) => {
	const fixture = await transferFixture(t);
	const reported: string[] = [];
	const server = await startTransferServer(fixture, async (input) => input.command, (error) => reported.push(String(error)));
	t.after(() => server.close());
	const client = await JsonlClient.connect(fixture.socketPath);
	t.after(() => client.close());

	const cases = [
		{
			name: "expired",
			code: "lease_expired",
			make: (requestId: string) => writeLease(fixture, { requestId, expiresAt: Date.now() - 1 }),
		},
		{
			name: "request mismatch",
			code: "lease_mismatch",
			make: (_requestId: string) => writeLease(fixture, { requestId: randomUUID() }),
		},
		{
			name: "thread mismatch",
			code: "lease_mismatch",
			make: (requestId: string) => writeLease(fixture, { requestId, threadId: randomUUID() }),
		},
		{
			name: "target mismatch",
			code: "lease_mismatch",
			make: (requestId: string) => writeLease(fixture, {
				requestId,
				target: { stream_id: "other-stream", session_id: fixture.host.identity.sessionId, session_epoch: fixture.host.identity.sessionEpoch },
			}),
		},
		{
			name: "missing lease",
			code: "lease_not_found",
			make: async (_requestId: string) => randomUUID(),
		},
		{
			name: "digest mismatch",
			code: "lease_corrupt",
			make: (requestId: string) => writeLease(fixture, { requestId, sha256: "0".repeat(64) }),
		},
		{
			name: "symlink payload",
			code: "lease_corrupt",
			make: (requestId: string) => writeLease(fixture, { requestId, payloadSymlink: true }),
		},
		{
			name: "oversized metadata",
			code: "lease_corrupt",
			make: (requestId: string) => writeLease(fixture, { requestId, declaredSize: ATTACHMENT_CAPABILITY.maxFileBytes + 1 }),
		},
		{
			name: "unknown metadata field",
			code: "lease_corrupt",
			make: async (requestId: string) => {
				const leaseId = await writeLease(fixture, { requestId });
				const metadataPath = path.join(fixture.inbox, "leases", leaseId, "lease.json");
				const metadata = JSON.parse(await fs.promises.readFile(metadataPath, "utf8"));
				metadata.browser_path = "/must/not/be/accepted";
				await fs.promises.writeFile(metadataPath, JSON.stringify(metadata), { mode: 0o600 });
				return leaseId;
			},
		},
	];

	for (const entry of cases) {
		const requestId = randomUUID();
		const leaseId = await entry.make(requestId);
		client.send({
			protocol: "live-v1",
			id: requestId,
			op: "dispatch",
			target: fixture.host.identity,
			command: { type: "prompt", message: entry.name },
			attachments: [{ leaseId }],
		});
		const failed = await client.next();
		assert.equal(failed.error.code, entry.code, entry.name);
		assert.ok(!JSON.stringify(failed).includes(fixture.root), entry.name);
		client.send({ protocol: "live-v1", id: `lookup-${entry.name.replaceAll(" ", "-")}`, op: "get_result", target: fixture.host.identity, requestId });
		const retained = await client.next();
		assert.equal(retained.result.status, "failed", entry.name);
		assert.equal(retained.result.error.code, entry.code, entry.name);
		client.send({
			protocol: "live-v1",
			id: requestId,
			op: "dispatch",
			target: fixture.host.identity,
			command: { type: "prompt", message: "must not retry" },
			attachments: [{ leaseId }],
		});
		assert.equal((await client.next()).error.code, "duplicate_request", entry.name);
	}
	const aggregateRequest = randomUUID();
	const full = Buffer.alloc(ATTACHMENT_CAPABILITY.maxFileBytes, 0x61);
	const aggregateLeases = [
		await writeLease(fixture, { requestId: aggregateRequest, bytes: full, name: "one.bin" }),
		await writeLease(fixture, { requestId: aggregateRequest, bytes: full, name: "two.bin" }),
		await writeLease(fixture, { requestId: aggregateRequest, bytes: Buffer.from("x"), name: "three.bin" }),
	];
	client.send({
		protocol: "live-v1",
		id: aggregateRequest,
		op: "dispatch",
		target: fixture.host.identity,
		command: { type: "prompt", message: "aggregate bound" },
		attachments: aggregateLeases.map((leaseId) => ({ leaseId })),
	});
	assert.equal((await client.next(10_000)).error.code, "lease_mismatch");

	assert.equal(fixture.host.dispatchCalls.length, 0);
	assert.deepEqual(reported, []);
});

test("attachment mutation identity is reserved before preparation and survives disconnect uncertainty", async (t) => {
	const fixture = await transferFixture(t);
	const requestId = randomUUID();
	const leaseId = await writeLease(fixture, { requestId, bytes: Buffer.from("uncertain attachment") });
	let preparationStarted!: () => void;
	let releasePreparation!: () => void;
	const started = new Promise<void>((resolve) => { preparationStarted = resolve; });
	const release = new Promise<void>((resolve) => { releasePreparation = resolve; });
	const server = await startTransferServer(fixture, async (input) => {
		preparationStarted();
		await release;
		return input.command;
	});
	t.after(() => server.close());

	const first = await JsonlClient.connect(fixture.socketPath);
	first.send({
		protocol: "live-v1",
		id: requestId,
		op: "dispatch",
		target: fixture.host.identity,
		command: { type: "prompt", message: "uncertain" },
		attachments: [{ leaseId }],
	});
	await started;
	first.close();

	const reconciler = await JsonlClient.connect(fixture.socketPath);
	t.after(() => reconciler.close());
	reconciler.send({
		protocol: "live-v1",
		id: requestId,
		op: "dispatch",
		target: fixture.host.identity,
		command: { type: "prompt", message: "must not replay" },
		attachments: [{ leaseId }],
	});
	assert.equal((await reconciler.next()).error.code, "duplicate_request");
	releasePreparation();

	let result: any;
	for (let attempt = 0; attempt < 20; attempt++) {
		reconciler.send({
			protocol: "live-v1",
			id: `poll-${attempt}`,
			op: "get_result",
			target: fixture.host.identity,
			requestId,
		});
		result = await reconciler.next();
		if (result.result?.status === "completed") break;
		await new Promise((resolve) => setTimeout(resolve, 10));
	}
	assert.equal(result.result.status, "completed");
	assert.equal(fixture.host.dispatchCalls.length, 1);
});

test("unsupported and malformed attachment requests preserve ordinary live-v1", async (t) => {
	const fixture = await transferFixture(t);
	const reported: string[] = [];
	const symlinkInbox = path.join(fixture.root, "symlink-inbox");
	await fs.promises.symlink(fixture.inbox, symlinkInbox);
	const server = await startLiveTransport(fixture.host, {
		socketPath: fixture.socketPath,
		onError: (error) => reported.push(String(error)),
		attachments: {
			threadId: fixture.threadId,
			inboxPath: symlinkInbox,
			apiSocketPath: fixture.apiSocketPath,
			prepareRpcPromptFiles: async (input) => input.command,
		},
	});
	t.after(() => server.close());
	const client = await JsonlClient.connect(fixture.socketPath);
	t.after(() => client.close());

	client.send({ protocol: "live-v1", id: "caps", op: "capabilities" });
	assert.equal((await client.next()).result.attachments, undefined);
	assert.equal(reported.length, 1);
	assert.ok(!reported[0]!.includes(fixture.root));
	client.send({
		protocol: "live-v1",
		id: randomUUID(),
		op: "dispatch",
		target: fixture.host.identity,
		command: { type: "prompt", message: "attachment unavailable" },
		attachments: [{ leaseId: randomUUID() }],
	});
	assert.equal((await client.next()).error.code, "transfer_unavailable");
	assert.equal(fixture.host.dispatchCalls.length, 0);

	client.send({
		protocol: "live-v1",
		id: "ordinary-write",
		op: "dispatch",
		target: fixture.host.identity,
		command: { type: "prompt", message: "ordinary still works" },
	});
	assert.equal((await client.next()).result.status, "completed");
	assert.equal(fixture.host.dispatchCalls.length, 1);

	for (const malformed of [
		{ id: randomUUID(), attachments: [] },
		{ id: "not-a-uuid", attachments: [{ leaseId: randomUUID() }] },
		{ id: randomUUID(), attachments: [{ leaseId: "not-a-uuid" }] },
		{ id: randomUUID(), attachments: [{ leaseId: randomUUID(), path: "/browser/path" }] },
		{ id: randomUUID(), attachments: Array.from({ length: 9 }, () => ({ leaseId: randomUUID() })) },
	]) {
		client.send({
			protocol: "live-v1",
			id: malformed.id,
			op: "dispatch",
			target: fixture.host.identity,
			command: { type: "prompt", message: "invalid" },
			attachments: malformed.attachments,
		});
		assert.equal((await client.next()).error.code, "invalid_request");
	}

	client.send({
		protocol: "live-v1",
		id: "query-with-attachments",
		op: "query",
		target: fixture.host.identity,
		command: { type: "get_state" },
		attachments: [{ leaseId: randomUUID() }],
	});
	assert.equal((await client.next()).error.code, "invalid_request");
	client.send({
		protocol: "live-v1",
		id: randomUUID(),
		op: "dispatch",
		target: fixture.host.identity,
		command: { type: "abort" },
		attachments: [{ leaseId: randomUUID() }],
	});
	assert.equal((await client.next()).error.code, "invalid_request");
});

test("cleanup removes only its exact consumer marker", async (t) => {
	const fixture = await transferFixture(t);
	const server = await startTransferServer(fixture, async (input) => input.command);
	const markerPath = path.join(fixture.inbox, "consumer.json");
	const replacement = {
		protocol: "sesh.pi-transfer",
		version: 1,
		thread_id: fixture.threadId,
		target: { stream_id: "replacement", session_id: "replacement-session", session_epoch: 3 },
		capabilities: ["attachment-lease-v1", "artifact-publish-v1"],
	};
	await fs.promises.writeFile(markerPath, JSON.stringify(replacement), { mode: 0o600 });
	await fs.promises.chmod(markerPath, 0o600);
	await server.close();
	assert.deepEqual(JSON.parse(await fs.promises.readFile(markerPath, "utf8")), replacement);
});
