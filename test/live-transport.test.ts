import { test, type TestContext } from "node:test";
import assert from "node:assert/strict";
import * as net from "node:net";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { EventEmitter, once } from "node:events";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import {
	startLiveTransport,
	probeSocketPath,
	type LiveEvent,
	type InputImageCapableLiveTransportHost,
	type LiveIdentity,
	type LiveSnapshot,
	type LiveInputImageCapability,
	type LiveOutputArtifactPresentationCapability,
	type LiveTransportHost,
} from "../live-transport.ts";
import extensionFactory from "../index.ts";
import { liveSocketPath, pickSocketsDir } from "../socket-paths.ts";

const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

class FakeHost implements LiveTransportHost {
	readonly identity: LiveIdentity = { streamId: "stream-1", sessionEpoch: 0, sessionId: "session-1" };
	sequence = 0;
	closed = false;
	dispatchCalls: Array<{ target: LiveIdentity; command: unknown }> = [];
	snapshotHook?: (watermark: number) => void;
	snapshotExtra: Record<string, unknown> = {};
	dispatchImpl: LiveTransportHost["dispatch"] = async (_target, command) => ({
		id: (command as { id: string }).id,
		type: (command as { type: string }).type,
		success: true,
	});
	private listeners = new Set<(event: LiveEvent) => void>();

	snapshot(): LiveSnapshot {
		const watermark = this.sequence;
		this.snapshotHook?.(watermark);
		return {
			...this.identity,
			sequence: watermark,
			state: { idle: true },
			messages: [],
			messageCoverage: {
				startIndex: 0,
				endIndexExclusive: 0,
				totalMessages: 0,
				byteLength: 2,
				hasOlder: false,
				hasNewer: false,
			},
			pendingToolCalls: [],
			steering: [],
			followUp: [],
			...this.snapshotExtra,
		} as LiveSnapshot;
	}

	subscribe(listener: (event: LiveEvent) => void): () => void {
		this.listeners.add(listener);
		return () => this.listeners.delete(listener);
	}

	dispatch(target: LiveIdentity, command: unknown, output: (response: unknown) => void): Promise<unknown | undefined> {
		this.dispatchCalls.push({ target, command });
		return this.dispatchImpl(target, command, output);
	}

	emit(type: string, data: Record<string, unknown> = {}): LiveEvent {
		const event = { ...this.identity, sequence: ++this.sequence, type, ...data } as LiveEvent;
		for (const listener of [...this.listeners]) listener(event);
		return event;
	}

	close(): void {
		this.closed = true;
		this.listeners.clear();
	}
}

const INPUT_IMAGE_CAPABILITY: LiveInputImageCapability = {
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
};

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

const outputArtifactPresentationSnapshot = (capability: unknown = structuredClone(OUTPUT_ARTIFACT_PRESENTATION_CAPABILITY)) => ({
	outputArtifactPresentationCapability: capability,
});

const inputImageSnapshot = () => ({
	inputImageCapability: { ...INPUT_IMAGE_CAPABILITY },
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

function addInputImageCopy(host: FakeHost): asserts host is FakeHost & InputImageCapableLiveTransportHost {
	Object.assign(host, {
		getCurrentBranchInputImage: () => { throw Object.assign(new Error("missing"), { code: "input_image_not_found" }); },
		copyInputImageBytes: async () => ({ byteLength: 0, sha256: "0".repeat(64) }),
	});
}

function makeInputImageHost(): FakeHost & InputImageCapableLiveTransportHost {
	const host = new FakeHost();
	host.snapshotExtra = inputImageSnapshot();
	addInputImageCopy(host);
	return host;
}

class JsonlClient {
	private frames: any[] = [];
	private waiters: Array<() => void> = [];
	private buffer = "";
	readonly socket: net.Socket;

	private constructor(socket: net.Socket) {
		this.socket = socket;
		socket.setEncoding("utf8");
		socket.on("data", (chunk) => {
			this.buffer += chunk;
			const lines = this.buffer.split("\n");
			this.buffer = lines.pop()!;
			for (const line of lines) {
				if (line) this.frames.push(JSON.parse(line));
			}
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

async function fixture(
	t: TestContext,
	options: Parameters<typeof startLiveTransport>[1]["limits"] = {},
	providedHost?: FakeHost,
) {
	const directory = await fs.promises.mkdtemp(path.join(os.tmpdir(), "pi-live-transport-"));
	const socketPath = path.join(directory, "fixture.live.sock");
	const host = providedHost ?? new FakeHost();
	const server = await startLiveTransport(host, { socketPath, limits: options });
	t.after(async () => {
		await server.close();
		await fs.promises.rm(directory, { recursive: true, force: true });
	});
	return { directory, socketPath, host, server };
}

test("socket probing unlinks only confirmed stale sockets and refuses ambiguous failures", async () => {
	const connector = (outcome: "connect" | "timeout" | NodeJS.ErrnoException) => () => {
		const emitter = new EventEmitter();
		const socket = Object.assign(emitter, {
			destroy: () => socket,
			setTimeout: (_milliseconds: number, callback: () => void) => {
				if (outcome === "timeout") queueMicrotask(callback);
				return socket;
			},
		}) as unknown as net.Socket;
		if (outcome === "connect") queueMicrotask(() => emitter.emit("connect"));
		else if (outcome !== "timeout") queueMicrotask(() => emitter.emit("error", outcome));
		return socket;
	};

	assert.equal(await probeSocketPath("/fixture", 10, connector("connect")), "active");
	for (const code of ["ECONNREFUSED", "ENOENT"]) {
		const error = Object.assign(new Error(code), { code }) as NodeJS.ErrnoException;
		assert.equal(await probeSocketPath("/fixture", 10, connector(error)), "stale");
	}
	const permission = Object.assign(new Error("denied"), { code: "EACCES" }) as NodeJS.ErrnoException;
	await assert.rejects(probeSocketPath("/fixture", 10, connector(permission)), /Cannot safely classify/);
	await assert.rejects(probeSocketPath("/fixture", 10, connector("timeout")), /refusing to unlink/);
});

test("a confirmed ECONNREFUSED socket is cleaned up without touching active listeners", async (t) => {
	const directory = await fs.promises.mkdtemp(path.join(os.tmpdir(), "pi-live-stale-"));
	const socketPath = path.join(directory, "stale.live.sock");
	const child = spawn(process.execPath, [
		"-e",
		"const net=require('node:net');const server=net.createServer();server.listen(process.argv[1],()=>process.stdout.write('ready\\n'));",
		socketPath,
	], { stdio: ["ignore", "pipe", "pipe"] });
	t.after(() => { if (child.exitCode === null) child.kill("SIGKILL"); });
	await new Promise<void>((resolve, reject) => {
		const timer = setTimeout(() => reject(new Error("stale socket fixture did not bind")), 2000);
		child.stdout!.once("data", () => {
			clearTimeout(timer);
			resolve();
		});
		child.once("error", (error) => {
			clearTimeout(timer);
			reject(error);
		});
	});
	child.kill("SIGKILL");
	await once(child, "exit");
	assert.equal(fs.existsSync(socketPath), true);

	const host = new FakeHost();
	const server = await startLiveTransport(host, { socketPath });
	await server.close();
	assert.equal(fs.existsSync(socketPath), false);
	await fs.promises.rm(directory, { recursive: true, force: true });
});

test("subscribe sends a coherent snapshot, reentrant tail events, bounded replay and explicit gaps", async (t) => {
	const { socketPath, host } = await fixture(t, { maxReplayEvents: 2, maxReplayBytes: 1024 * 1024 });
	let hooked = false;
	host.snapshotHook = () => {
		if (hooked) return;
		hooked = true;
		host.emit("session_event", { origin: "interactive" });
	};

	const first = await JsonlClient.connect(socketPath);
	t.after(() => first.close());
	first.send({ protocol: "live-v1", id: "sub-1", op: "subscribe" });
	assert.equal((await first.next()).result.mode, "snapshot");
	const snapshot = await first.next();
	assert.equal(snapshot.type, "snapshot");
	assert.equal(snapshot.snapshot.sequence, 0);
	const reentrant = await first.next();
	assert.equal(reentrant.type, "event");
	assert.equal(reentrant.event.sequence, 1);
	assert.equal(reentrant.event.origin, "interactive");

	host.emit("session_event", { origin: "extension" });
	assert.equal((await first.next()).event.origin, "extension");
	first.close();
	host.emit("session_event", { n: 3 });
	host.emit("session_event", { n: 4 });
	host.emit("session_event", { n: 5 });

	const missed = await JsonlClient.connect(socketPath);
	t.after(() => missed.close());
	missed.send({ protocol: "live-v1", id: "sub-2", op: "subscribe", after: { streamId: "stream-1", sequence: 1 } });
	assert.equal((await missed.next()).result.mode, "snapshot");
	const gap = await missed.next();
	assert.equal(gap.type, "gap");
	assert.equal(gap.gap.reason, "replay_unavailable");
	assert.equal((await missed.next()).snapshot.sequence, 5);

	const resumed = await JsonlClient.connect(socketPath);
	t.after(() => resumed.close());
	resumed.send({ protocol: "live-v1", id: "sub-3", op: "subscribe", after: { streamId: "stream-1", sequence: 4 } });
	assert.equal((await resumed.next()).result.mode, "replay");
	const replayed = await resumed.next();
	assert.equal(replayed.event.sequence, 5);
	assert.equal(replayed.event.n, 5);
});

test("transient auth notifications reach current observers but create a replay barrier", async (t) => {
	const directory = await fs.promises.mkdtemp(path.join(os.tmpdir(), "pi-live-auth-replay-"));
	const socketPath = path.join(directory, "fixture.live.sock");
	const host = new FakeHost();
	const reportedErrors: string[] = [];
	const server = await startLiveTransport(host, {
		socketPath,
		limits: { maxResults: 1, maxResultBytes: 4096 },
		onError: (error) => reportedErrors.push(error instanceof Error ? error.message : String(error)),
	});
	t.after(async () => {
		await server.close();
		await fs.promises.rm(directory, { recursive: true, force: true });
	});
	const authUrl = "https://auth.example.test/authorize?secret=oauth-secret";
	const deviceCode = "DEVICE-CODE-SECRET";
	const instructions = "Enter the transient code only in the trusted browser";
	const assertNoAuthPayload = (value: unknown) => {
		const encoded = JSON.stringify(value);
		assert.ok(!encoded.includes(authUrl), encoded);
		assert.ok(!encoded.includes(deviceCode), encoded);
		assert.ok(!encoded.includes(instructions), encoded);
	};

	const current = await JsonlClient.connect(socketPath);
	t.after(() => current.close());
	current.send({ protocol: "live-v1", id: "auth-current", op: "subscribe" });
	assert.equal((await current.next()).result.mode, "snapshot");
	await current.next();
	const beforeAuth = host.emit("runtime_state", { state: "ready-before-auth" });
	const urlEvent = host.emit("auth_notification", {
		notification: { type: "auth_url", url: authUrl, instructions },
	});
	const betweenAuth = host.emit("runtime_state", { state: "ready-between-auth" });
	const codeEvent = host.emit("auth_notification", {
		notification: { type: "device_code", userCode: deviceCode, verificationUri: "https://device.example.test" },
	});
	const afterAuth = host.emit("runtime_state", { state: "ready-after-auth" });
	assert.deepEqual((await current.next()).event, beforeAuth);
	assert.deepEqual((await current.next()).event, urlEvent);
	assert.deepEqual((await current.next()).event, betweenAuth);
	assert.deepEqual((await current.next()).event, codeEvent);
	assert.deepEqual((await current.next()).event, afterAuth);
	current.close();

	const oldCursor = await JsonlClient.connect(socketPath);
	t.after(() => oldCursor.close());
	oldCursor.send({
		protocol: "live-v1",
		id: "auth-old",
		op: "subscribe",
		after: { streamId: host.identity.streamId, sequence: 0 },
	});
	const oldResponse = await oldCursor.next();
	const oldGap = await oldCursor.next();
	const oldSnapshot = await oldCursor.next();
	assert.equal(oldResponse.result.mode, "snapshot");
	assert.equal(oldGap.type, "gap");
	assert.equal(oldGap.gap.reason, "replay_unavailable");
	assert.equal(oldSnapshot.snapshot.sequence, afterAuth.sequence);
	assertNoAuthPayload([oldResponse, oldGap, oldSnapshot]);

	const crossedBarrier = await JsonlClient.connect(socketPath);
	t.after(() => crossedBarrier.close());
	crossedBarrier.send({
		protocol: "live-v1",
		id: "auth-crossed",
		op: "subscribe",
		after: { streamId: host.identity.streamId, sequence: betweenAuth.sequence },
	});
	const crossedResponse = await crossedBarrier.next();
	const crossedGap = await crossedBarrier.next();
	const crossedSnapshot = await crossedBarrier.next();
	assert.equal(crossedResponse.result.mode, "snapshot");
	assert.equal(crossedGap.type, "gap");
	assertNoAuthPayload([crossedResponse, crossedGap, crossedSnapshot]);

	const afterBarrier = await JsonlClient.connect(socketPath);
	t.after(() => afterBarrier.close());
	afterBarrier.send({
		protocol: "live-v1",
		id: "auth-after",
		op: "subscribe",
		after: { streamId: host.identity.streamId, sequence: codeEvent.sequence },
	});
	const replayResponse = await afterBarrier.next();
	const replayed = await afterBarrier.next();
	assert.equal(replayResponse.result.mode, "replay");
	assert.deepEqual(replayed.event, afterAuth);
	assertNoAuthPayload([replayResponse, replayed]);

	afterBarrier.send({
		protocol: "live-v1",
		id: "auth-result-check",
		op: "get_result",
		target: host.identity,
		requestId: "auth-notification",
	});
	const missingResult = await afterBarrier.next();
	assert.equal(missingResult.error.code, "result_not_found");
	assertNoAuthPayload(missingResult);

	afterBarrier.send({
		protocol: "live-v1",
		id: "auth-ledger-write",
		op: "dispatch",
		target: host.identity,
		command: { type: "abort", id: "native-after-auth" },
	});
	const retainedMutation = await afterBarrier.next();
	assert.equal(retainedMutation.result.status, "completed");
	assertNoAuthPayload(retainedMutation);
	assert.equal(host.dispatchCalls.length, 1);
	assert.deepEqual(reportedErrors, []);
});

test("revisioned standard picker snapshots and interaction_updated events cross the wire unchanged", async (t) => {
	const { socketPath, host } = await fixture(t);
	const scopedInitial = {
		interactionId: "scoped-1",
		revision: 1,
		kind: "scoped_models",
		title: "Scoped models",
		browserAnswerable: true,
		state: {
			models: [
				{ id: "provider/model", provider: "provider", modelId: "model", name: "Model", available: true },
				{ id: "missing/*", available: false },
			],
			enabledModelIds: null,
			dirty: false,
			refresh: { status: "complete", message: "2 models" },
		},
	};
	const settingsInitial = {
		interactionId: "settings-1",
		revision: 3,
		kind: "settings",
		title: "Settings",
		settings: [
			{ id: "queue", label: "Queue mode", description: "Delivery order", currentValue: "all", values: ["all", "one-at-a-time"] },
		],
		modelThinking: {
			defaultLevel: "medium",
			models: [
				{
					id: "provider/model",
					provider: "provider",
					modelId: "model",
					label: "Model",
					supportedLevels: ["off", "medium", "high"],
					override: "high",
				},
			],
		},
		theme: {
			current: { mode: "automatic", lightTheme: "light", darkTheme: "dark" },
			availableThemes: ["light", "dark"],
			terminalTheme: "dark",
		},
	};
	host.snapshotExtra = { pendingInteractions: [scopedInitial, settingsInitial] };

	const live = await JsonlClient.connect(socketPath);
	t.after(() => live.close());
	live.send({ protocol: "live-v1", id: "picker-sub", op: "subscribe" });
	assert.equal((await live.next()).result.mode, "snapshot");
	const snapshot = await live.next();
	assert.deepEqual(snapshot.snapshot.pendingInteractions, [scopedInitial, settingsInitial]);

	const scopedUpdated = {
		...scopedInitial,
		revision: 2,
		state: { ...scopedInitial.state, enabledModelIds: ["provider/model"], dirty: true },
	};
	const settingsUpdated = {
		...settingsInitial,
		revision: 4,
		theme: { ...settingsInitial.theme, current: { mode: "single", theme: "dark" } },
	};
	const scopedEvent = host.emit("interaction_updated", { interaction: scopedUpdated, source: "remote" });
	const settingsEvent = host.emit("interaction_updated", { interaction: settingsUpdated, source: "terminal" });
	assert.deepEqual((await live.next()).event, scopedEvent);
	assert.deepEqual((await live.next()).event, settingsEvent);
	live.close();

	const replay = await JsonlClient.connect(socketPath);
	t.after(() => replay.close());
	replay.send({ protocol: "live-v1", id: "picker-replay", op: "subscribe", after: { streamId: host.identity.streamId, sequence: 0 } });
	assert.equal((await replay.next()).result.mode, "replay");
	assert.deepEqual((await replay.next()).event, scopedEvent);
	assert.deepEqual((await replay.next()).event, settingsEvent);
});

test("an empty replay buffer gaps an older checkpoint instead of claiming empty replay", async (t) => {
	const directory = await fs.promises.mkdtemp(path.join(os.tmpdir(), "pi-live-empty-replay-"));
	const socketPath = path.join(directory, "fixture.live.sock");
	const host = new FakeHost();
	host.sequence = 7;
	const server = await startLiveTransport(host, { socketPath });
	t.after(async () => {
		await server.close();
		await fs.promises.rm(directory, { recursive: true, force: true });
	});

	const client = await JsonlClient.connect(socketPath);
	t.after(() => client.close());
	client.send({ protocol: "live-v1", id: "empty", op: "subscribe", after: { streamId: "stream-1", sequence: 6 } });
	assert.equal((await client.next()).result.mode, "snapshot");
	const gap = await client.next();
	assert.equal(gap.type, "gap");
	assert.equal(gap.gap.reason, "replay_unavailable");
	assert.equal((await client.next()).snapshot.sequence, 7);
});

test("one-MiB frame limits refuse oversized snapshots and gap oversized events without truncation", async (t) => {
	const { socketPath, host } = await fixture(t);
	host.snapshotExtra = { oversizedHistory: "x".repeat(1024 * 1024) };
	const history = await JsonlClient.connect(socketPath);
	t.after(() => history.close());
	history.send({ protocol: "live-v1", id: "history", op: "subscribe" });
	const historyError = await history.next();
	assert.equal(historyError.ok, false);
	assert.equal(historyError.error.code, "frame_too_large");

	host.snapshotExtra = {};
	const live = await JsonlClient.connect(socketPath);
	t.after(() => live.close());
	live.send({ protocol: "live-v1", id: "live", op: "subscribe" });
	assert.equal((await live.next()).result.mode, "snapshot");
	await live.next();
	host.emit("session_event", {
		event: { type: "agent_end", messages: [{ role: "assistant", content: "x".repeat(1024 * 1024) }] },
	});
	const gap = await live.next();
	assert.equal(gap.type, "gap");
	assert.equal(gap.gap.reason, "event_too_large");
	assert.equal((await live.next()).type, "snapshot");
	live.close();

	const reconnect = await JsonlClient.connect(socketPath);
	t.after(() => reconnect.close());
	reconnect.send({ protocol: "live-v1", id: "reconnect", op: "subscribe", after: { streamId: "stream-1", sequence: 0 } });
	assert.equal((await reconnect.next()).result.mode, "snapshot");
	assert.equal((await reconnect.next()).type, "gap");
	assert.equal((await reconnect.next()).snapshot.sequence, 1);
});

test("bounded transcript snapshot metadata and history pages cross the wire without silent truncation", async (t) => {
	const { socketPath, host } = await fixture(t);
	const latest = { role: "user", content: [{ type: "text", text: "latest" }], timestamp: 1 };
	host.snapshotExtra = {
		messages: [latest],
		messageCoverage: {
			startIndex: 11,
			endIndexExclusive: 12,
			totalMessages: 12,
			byteLength: Buffer.byteLength(JSON.stringify([latest])),
			hasOlder: true,
			hasNewer: false,
		},
		historyCursor: { leafId: "leaf-1", before: 11 },
	};
	const historicalText = "h".repeat(700 * 1024);
	const historical = { role: "assistant", content: [{ type: "text", text: historicalText }], timestamp: 2 };
	const historyBytes = Buffer.byteLength(JSON.stringify([historical]));
	host.dispatchImpl = async (_target, command) => {
		const typed = command as { type: string; cursor?: unknown; maxMessages?: number; maxBytes?: number };
		assert.equal(typed.type, "history");
		assert.deepEqual(typed.cursor, { leafId: "leaf-1", before: 11 });
		assert.equal(typed.maxMessages, 1);
		assert.equal(typed.maxBytes, 768 * 1024);
		return {
			messages: [historical],
			coverage: {
				startIndex: 10,
				endIndexExclusive: 11,
				totalMessages: 12,
				byteLength: historyBytes,
				hasOlder: true,
				hasNewer: true,
			},
			nextCursor: { leafId: "leaf-1", before: 10 },
		};
	};

	const client = await JsonlClient.connect(socketPath);
	t.after(() => client.close());
	client.send({ protocol: "live-v1", id: "snapshot-page", op: "snapshot" });
	const snapshotReply = await client.next();
	assert.deepEqual(snapshotReply.result.snapshot.messageCoverage, host.snapshotExtra.messageCoverage);
	assert.deepEqual(snapshotReply.result.snapshot.historyCursor, { leafId: "leaf-1", before: 11 });

	client.send({
		protocol: "live-v1",
		id: "history-page",
		op: "dispatch",
		target: host.identity,
		command: {
			type: "history",
			cursor: { leafId: "leaf-1", before: 11 },
			maxMessages: 1,
			maxBytes: 768 * 1024,
		},
	});
	const historyReply = await client.next();
	assert.equal(historyReply.ok, true);
	assert.equal(historyReply.result.response.messages[0].content[0].text.length, historicalText.length);
	assert.deepEqual(historyReply.result.response.coverage, {
		startIndex: 10,
		endIndexExclusive: 11,
		totalMessages: 12,
		byteLength: historyBytes,
		hasOlder: true,
		hasNewer: true,
	});
	assert.deepEqual(historyReply.result.response.nextCursor, { leafId: "leaf-1", before: 10 });
	assert.ok(Buffer.byteLength(JSON.stringify(historyReply) + "\n") <= 1024 * 1024);

	const invalidHistory = [
		{ type: "history", cursor: { leafId: "leaf-1" } },
		{ type: "history", cursor: { leafId: 1, before: 11 } },
		{ type: "history", cursor: { leafId: "", before: 11 } },
		{ type: "history", cursor: { leafId: "bad\nleaf", before: 11 } },
		{ type: "history", cursor: { leafId: "x".repeat(513), before: 11 } },
		{ type: "history", cursor: { leafId: null, before: -1 } },
		{ type: "history", cursor: { leafId: null, before: 1.5 } },
		{ type: "history", cursor: { leafId: null, before: 11 }, maxMessages: 0 },
		{ type: "history", cursor: { leafId: null, before: 11 }, maxMessages: 501 },
		{ type: "history", cursor: { leafId: null, before: 11 }, maxBytes: 1 },
		{ type: "history", cursor: { leafId: null, before: 11 }, maxBytes: 768 * 1024 + 1 },
		{ type: "history", cursor: { leafId: null, before: 11 }, extra: true },
	];
	const acceptedBefore = host.dispatchCalls.length;
	for (const [index, command] of invalidHistory.entries()) {
		client.send({ protocol: "live-v1", id: `bad-history-${index}`, op: "dispatch", target: host.identity, command });
		assert.equal((await client.next()).error.code, "invalid_request");
	}
	assert.equal(host.dispatchCalls.length, acceptedBefore);
});

test("every native RPC command shape is type-checked while Pi retains command semantics", async (t) => {
	const { socketPath, host } = await fixture(t);
	const client = await JsonlClient.connect(socketPath);
	t.after(() => client.close());
	const interactionUpdates = [
		{ type: "interaction_update", interactionId: "picker-1", expectedRevision: 1, action: { domain: "scoped_models", action: "toggle", modelId: "provider/model" } },
		{ type: "interaction_update", interactionId: "picker-1", expectedRevision: 2, action: { domain: "scoped_models", action: "enable", query: "sonnet" } },
		{ type: "interaction_update", interactionId: "picker-1", expectedRevision: 3, action: { domain: "scoped_models", action: "clear" } },
		{ type: "interaction_update", interactionId: "picker-1", expectedRevision: 4, action: { domain: "scoped_models", action: "toggle_provider", provider: "provider" } },
		{ type: "interaction_update", interactionId: "picker-1", expectedRevision: 5, action: { domain: "scoped_models", action: "move", modelId: "provider/model", direction: "up" } },
		{ type: "interaction_update", interactionId: "picker-1", expectedRevision: 5, action: { domain: "scoped_models", action: "move", modelId: "provider/model", direction: "down" } },
		{ type: "interaction_update", interactionId: "picker-1", expectedRevision: 6, action: { domain: "scoped_models", action: "persist" } },
		{ type: "interaction_update", interactionId: "picker-1", expectedRevision: 7, action: { domain: "model_thinking", action: "set", modelId: "provider/model", level: "high" } },
		{ type: "interaction_update", interactionId: "picker-1", expectedRevision: 8, action: { domain: "model_thinking", action: "clear", modelId: "provider/model" } },
		{ type: "interaction_update", interactionId: "picker-1", expectedRevision: 9, action: { domain: "theme", action: "apply_single", theme: "dark" } },
		{ type: "interaction_update", interactionId: "picker-1", expectedRevision: 10, action: { domain: "theme", action: "apply_automatic", lightTheme: "light", darkTheme: "dark" } },
	] satisfies Array<Record<string, unknown>>;
	const commands: Array<Record<string, unknown>> = [
		{ type: "prompt", message: "", images: [{ type: "image", data: "AA==", mimeType: "image/png" }], streamingBehavior: "followUp" },
		{ type: "steer", message: "steer", images: [] },
		{ type: "follow_up", message: "later" },
		{ type: "abort" },
		{ type: "clear_queue" },
		{ type: "new_session", parentSession: "parent.jsonl" },
		{ type: "get_state" },
		{ type: "set_model", provider: "provider", modelId: "model" },
		{ type: "cycle_model" },
		{ type: "get_available_models" },
		{ type: "set_thinking_level", level: "max" },
		{ type: "cycle_thinking_level" },
		{ type: "get_available_thinking_levels" },
		{ type: "set_steering_mode", mode: "all" },
		{ type: "set_follow_up_mode", mode: "one-at-a-time" },
		{ type: "compact", customInstructions: "keep this" },
		{ type: "set_auto_compaction", enabled: true },
		{ type: "set_auto_retry", enabled: false },
		{ type: "abort_retry" },
		{ type: "bash", command: "printf ok", excludeFromContext: true },
		{ type: "abort_bash" },
		{ type: "get_session_stats" },
		{ type: "export_html", outputPath: "out.html" },
		{ type: "switch_session", sessionPath: "session.jsonl" },
		{ type: "fork", entryId: "entry-1" },
		{ type: "clone" },
		{ type: "get_fork_messages" },
		{ type: "get_entries", since: "entry-1" },
		{ type: "get_tree" },
		{ type: "get_last_assistant_text" },
		{ type: "set_session_name", name: "demo" },
		{ type: "get_messages" },
		{ type: "get_commands" },
		{ type: "interactive_builtin", operationId: "builtin-op", command: "/name first line\nsecond line" },
		{ type: "interaction_response", interactionId: "interaction-1", response: { kind: "select", optionId: "model", actionId: "favorite" } },
		{ type: "interaction_response", interactionId: "interaction-1", response: { kind: "editor", value: "line one\nline two" } },
		{ type: "interaction_response", interactionId: "interaction-1", response: { kind: "setting", settingId: "queue", value: "all" } },
		{ type: "interaction_response", interactionId: "interaction-1", response: { kind: "complete" } },
		...interactionUpdates,
		{ type: "history", cursor: { leafId: null, before: 0 }, maxMessages: 1, maxBytes: 2 },
		{ type: "branch_history", cursor: { leafId: null, before: 0 }, maxItems: 1, maxBytes: 2 },
	];
	for (const [index, command] of commands.entries()) {
		client.send({ protocol: "live-v1", id: `shape-${index}`, op: "dispatch", target: host.identity, command });
		const reply = await client.next();
		assert.equal(reply.ok, true, JSON.stringify({ command, reply }));
	}
	const builtinCall = host.dispatchCalls.find(({ command }) => (command as { type?: string }).type === "interactive_builtin");
	assert.equal((builtinCall?.command as { command: string }).command, "/name first line\nsecond line");
	for (const update of interactionUpdates) {
		assert.ok(host.dispatchCalls.some(({ command }) => JSON.stringify(command) === JSON.stringify(update)));
	}

	const invalidCommands = [
		{ type: "set_auto_compaction", enabled: "yes" },
		{ type: "set_auto_retry", enabled: 1 },
		{ type: "set_model", provider: "provider", modelId: 3 },
		{ type: "prompt", message: 42 },
		{ type: "prompt", message: "image", images: [{ type: "image", data: 4, mimeType: "image/png" }] },
		{ type: "bash", command: "true", excludeFromContext: "yes" },
		{ type: "abort", extra: true },
		{ type: "not_a_native_command" },
		{ type: "interaction_response", interactionId: "i", response: { kind: "select", optionId: "x", surprise: true } },
		{ type: "interaction_update", interactionId: "i", action: { domain: "scoped_models", action: "persist" } },
		{ type: "interaction_update", interactionId: "i", expectedRevision: -1, action: { domain: "scoped_models", action: "persist" } },
		{ type: "interaction_update", interactionId: "i", expectedRevision: 1.5, action: { domain: "scoped_models", action: "persist" } },
		{ type: "interaction_update", interactionId: "i", expectedRevision: Number.MAX_SAFE_INTEGER + 1, action: { domain: "scoped_models", action: "persist" } },
		{ type: "interaction_update", interactionId: "i", expectedRevision: 1, action: { domain: "scoped_models", action: "toggle" } },
		{ type: "interaction_update", interactionId: "i", expectedRevision: 1, action: { domain: "scoped_models", action: "move", modelId: "m", direction: "sideways" } },
		{ type: "interaction_update", interactionId: "i", expectedRevision: 1, action: { domain: "scoped_models", action: "persist", modelId: "unexpected" } },
		{ type: "interaction_update", interactionId: "i", expectedRevision: 1, action: { domain: "model_thinking", action: "set", modelId: "m" } },
		{ type: "interaction_update", interactionId: "i", expectedRevision: 1, action: { domain: "theme", action: "apply_automatic", lightTheme: "light" } },
		{ type: "interaction_update", interactionId: "i", expectedRevision: 1, action: { domain: "unknown", action: "persist" } },
		{ type: "history", cursor: { leafId: null, before: 0 }, maxMessages: 501 },
		{ type: "branch_history", cursor: { leafId: null, before: 0 }, maxItems: 501 },
	];
	const acceptedBefore = host.dispatchCalls.length;
	for (const [index, command] of invalidCommands.entries()) {
		client.send({ protocol: "live-v1", id: `invalid-shape-${index}`, op: "dispatch", target: host.identity, command });
		const reply = await client.next();
		assert.equal(reply.error.code, "invalid_request", JSON.stringify({ command, reply }));
	}
	assert.equal(host.dispatchCalls.length, acceptedBefore);
});

test("dispatch is stale-safe, deduplicated and recoverable after an uncertain client disconnect", async (t) => {
	let clock = 10_000;
	const directory = await fs.promises.mkdtemp(path.join(os.tmpdir(), "pi-live-result-"));
	const socketPath = path.join(directory, "fixture.live.sock");
	const host = new FakeHost();
	const server = await startLiveTransport(host, {
		socketPath,
		now: () => clock,
		limits: { resultRetentionMs: 100 },
	});
	t.after(async () => {
		await server.close();
		await fs.promises.rm(directory, { recursive: true, force: true });
	});
	host.dispatchImpl = async (target, command, output) => {
		if (target.sessionId !== host.identity.sessionId) throw new Error("Live RPC target is stale; reconcile the session before submitting");
		const typed = command as { type: string; id?: string; operationId?: string; expectedRevision?: number };
		if (typed.type === "interactive_builtin") return { operationId: typed.operationId, status: "completed" };
		if (typed.type === "interaction_response") return { status: "accepted" };
		if (typed.type === "interaction_update") {
			return {
				status: "stale",
				interaction: {
					interactionId: "interaction-uuid",
					revision: (typed.expectedRevision ?? 0) + 1,
					kind: "scoped_models",
					title: "Scoped models",
					browserAnswerable: true,
					state: { models: [], enabledModelIds: null, dirty: false, refresh: { status: "complete", message: "Current" } },
				},
			};
		}
		setTimeout(() => output({ id: typed.id, type: "prompt", success: true, data: { disposition: "started" } }), 20);
		return undefined;
	};

	const originalTarget = { ...host.identity };
	const uncertain = await JsonlClient.connect(socketPath);
	uncertain.send({
		protocol: "live-v1",
		id: "write-1",
		op: "dispatch",
		target: host.identity,
		command: { type: "prompt", id: "native-1", message: "hello" },
	});
	for (let i = 0; i < 50 && host.dispatchCalls.length === 0; i++) await delay(2);
	assert.equal(host.dispatchCalls.length, 1);
	uncertain.close();
	await delay(40);

	const poll = await JsonlClient.connect(socketPath);
	t.after(() => poll.close());
	poll.send({ protocol: "live-v1", id: "poll-1", op: "get_result", target: host.identity, requestId: "write-1" });
	const recovered = await poll.next();
	assert.equal(recovered.result.status, "completed");
	assert.equal(recovered.result.response.data.disposition, "started");
	poll.send({
		protocol: "live-v1",
		id: "poll-wrong-stream",
		op: "get_result",
		target: { ...host.identity, streamId: "other-stream" },
		requestId: "write-1",
	});
	assert.equal((await poll.next()).error.code, "result_target_mismatch");

	poll.send({
		protocol: "live-v1",
		id: "write-1",
		op: "dispatch",
		target: host.identity,
		command: { type: "prompt", id: "native-duplicate", message: "again" },
	});
	const duplicate = await poll.next();
	assert.equal(duplicate.ok, false);
	assert.equal(duplicate.error.code, "duplicate_request");
	assert.equal(host.dispatchCalls.length, 1);
	host.identity.streamId = "stream-2";
	host.identity.sessionEpoch = 1;
	poll.send({ protocol: "live-v1", id: "poll-original-target", op: "get_result", target: originalTarget, requestId: "write-1" });
	assert.equal((await poll.next()).result.status, "completed");

	poll.send({
		protocol: "live-v1",
		id: "builtin-1",
		op: "dispatch",
		target: host.identity,
		command: { type: "interactive_builtin", operationId: "operation-1", command: "/model anthropic/example" },
	});
	const builtin = await poll.next();
	assert.equal(builtin.result.response.status, "completed");

	poll.send({
		protocol: "live-v1",
		id: "interaction-1",
		op: "dispatch",
		target: host.identity,
		command: { type: "interaction_response", interactionId: "interaction-uuid", response: { kind: "confirm", confirmed: true } },
	});
	const interaction = await poll.next();
	assert.equal(interaction.result.response.status, "accepted");

	poll.send({
		protocol: "live-v1",
		id: "interaction-bad",
		op: "dispatch",
		target: host.identity,
		command: { type: "interaction_response", interactionId: "interaction-uuid", response: { kind: "confirm", confirmed: "yes" } },
	});
	const invalidInteraction = await poll.next();
	assert.equal(invalidInteraction.error.code, "invalid_request");
	assert.equal(host.dispatchCalls.length, 3);

	poll.send({
		protocol: "live-v1",
		id: "interaction-update-stale",
		op: "dispatch",
		target: host.identity,
		command: {
			type: "interaction_update",
			interactionId: "interaction-uuid",
			expectedRevision: 7,
			action: { domain: "scoped_models", action: "persist" },
		},
	});
	const staleRevision = await poll.next();
	assert.equal(staleRevision.result.response.status, "stale");
	assert.equal(staleRevision.result.response.interaction.revision, 8);
	assert.equal(host.dispatchCalls.length, 4);

	poll.send({
		protocol: "live-v1",
		id: "stale-1",
		op: "dispatch",
		target: { ...host.identity, sessionId: "old-session" },
		command: { type: "abort", id: "native-stale" },
	});
	const stale = await poll.next();
	assert.equal(stale.ok, false);
	assert.equal(stale.error.code, "stale_target");

	clock += 101;
	poll.send({ protocol: "live-v1", id: "poll-expired", op: "get_result", target: host.identity, requestId: "write-1" });
	const expired = await poll.next();
	assert.equal(expired.error.code, "result_not_found");
});

test("completed mutations with oversized responses become unavailable tombstones without redispatch", async (t) => {
	const { socketPath, host } = await fixture(t, {
		maxInputBytes: 1024,
		maxFrameBytes: 512,
		maxClientQueueBytes: 2048,
	});
	host.dispatchImpl = async () => ({ payload: "x".repeat(1024) });

	const client = await JsonlClient.connect(socketPath);
	t.after(() => client.close());
	client.send({
		protocol: "live-v1",
		id: "large-result",
		op: "dispatch",
		target: host.identity,
		command: { type: "abort", id: "native-large" },
	});
	const oversized = await client.next();
	assert.equal(oversized.ok, true);
	assert.equal(oversized.result.status, "unavailable");
	assert.equal(oversized.result.outcome, "completed");
	assert.equal(oversized.result.reason, "response_too_large");
	client.send({ protocol: "live-v1", id: "poll-large", op: "get_result", target: host.identity, requestId: "large-result" });
	const stored = await client.next();
	assert.equal(stored.result.status, "unavailable");
	assert.equal(stored.result.reason, "response_too_large");
	client.send({
		protocol: "live-v1",
		id: "large-result",
		op: "dispatch",
		target: host.identity,
		command: { type: "abort", id: "native-large-duplicate" },
	});
	assert.equal((await client.next()).error.code, "duplicate_request");
	assert.equal(host.dispatchCalls.length, 1);

	const input = await JsonlClient.connect(socketPath);
	t.after(() => input.close());
	input.socket.write("x".repeat(1025));
	const rejected = await input.next();
	assert.equal(rejected.error.code, "invalid_request");
});

test("output-artifact presentation capability is exact, detached and independently gates artifact_history", async (t) => {
	const { states: _missingStates, ...missingStates } = structuredClone(OUTPUT_ARTIFACT_PRESENTATION_CAPABILITY);
	const cases: Array<[string, unknown, boolean]> = [
		["exact", structuredClone(OUTPUT_ARTIFACT_PRESENTATION_CAPABILITY), true],
		["absent", undefined, false],
		["wrong-protocol", { ...OUTPUT_ARTIFACT_PRESENTATION_CAPABILITY, protocol: "pi.output-artifacts" }, false],
		["wrong-version", { ...OUTPUT_ARTIFACT_PRESENTATION_CAPABILITY, version: 2 }, false],
		["wrong-block", { ...OUTPUT_ARTIFACT_PRESENTATION_CAPABILITY, messageBlock: "artifact_reference" }, false],
		["wrong-query", { ...OUTPUT_ARTIFACT_PRESENTATION_CAPABILITY, metadataQuery: "history" }, false],
		["wrong-coverage", { ...OUTPUT_ARTIFACT_PRESENTATION_CAPABILITY, coverage: "recursive" }, false],
		["missing-states", missingStates, false],
		["reordered-states", { ...OUTPUT_ARTIFACT_PRESENTATION_CAPABILITY, states: ["partial", "resolved", "missing", "duplicate", "unsupported"] }, false],
		["extra-state", { ...OUTPUT_ARTIFACT_PRESENTATION_CAPABILITY, states: [...OUTPUT_ARTIFACT_PRESENTATION_CAPABILITY.states, "other"] }, false],
		["wrong-nested", { ...OUTPUT_ARTIFACT_PRESENTATION_CAPABILITY, nestedToolResults: "resolved" }, false],
		["binary-live", { ...OUTPUT_ARTIFACT_PRESENTATION_CAPABILITY, binaryInLive: true }, false],
		["extra-field", { ...OUTPUT_ARTIFACT_PRESENTATION_CAPABILITY, extra: true }, false],
	];
	for (const [name, capability, expected] of cases) {
		const host = new FakeHost();
		if (capability !== undefined) host.snapshotExtra = outputArtifactPresentationSnapshot(capability);
		host.dispatchImpl = async () => ({ scope: "current_branch_artifacts", artifacts: [] });
		const { socketPath } = await fixture(t, {}, host);
		if (expected) {
			const nativeCapability = host.snapshotExtra.outputArtifactPresentationCapability as { states: string[] };
			nativeCapability.states[0] = "mutated-after-start";
		}
		const client = await JsonlClient.connect(socketPath);
		t.after(() => client.close());
		client.send({ protocol: "live-v1", id: `caps-output-${name}`, op: "capabilities" });
		const capabilities = (await client.next()).result;
		assert.equal(capabilities.queryCommandTypes.includes("artifact_history"), expected);
		assert.deepEqual(capabilities.outputArtifactPresentation, expected ? OUTPUT_ARTIFACT_PRESENTATION_CAPABILITY : undefined);
		assert.equal(capabilities.managedArtifacts, undefined);
		client.send({
			protocol: "live-v1", id: `artifact-query-${name}`, op: "query", target: host.identity,
			command: { type: "artifact_history", cursor: { leafId: null, before: 0 }, maxItems: 1, maxBytes: 2 },
		});
		const query = await client.next();
		assert.equal(query.ok, expected);
		if (!expected) assert.equal(query.error.code, "invalid_request");
		assert.equal(host.dispatchCalls.length, expected ? 1 : 0);
	}
});

test("artifact_history is a strictly validated capability-gated pure query", async (t) => {
	const host = new FakeHost();
	host.snapshotExtra = outputArtifactPresentationSnapshot();
	host.dispatchImpl = async (_target, command) => ({
		scope: "current_branch_artifacts",
		artifacts: [],
		coverage: {
			scope: "current_branch_artifacts", leafId: null, startIndex: 0, endIndexExclusive: 0,
			totalArtifacts: 0, byteLength: 2, hasOlder: false, hasNewer: false,
		},
		commandType: (command as { type: string }).type,
	});
	const { socketPath } = await fixture(t, {}, host);
	const client = await JsonlClient.connect(socketPath);
	t.after(() => client.close());
	const valid = { type: "artifact_history", cursor: { leafId: null, before: 0 }, maxItems: 500, maxBytes: 512 * 1024 };
	for (let attempt = 0; attempt < 2; attempt++) {
		client.send({ protocol: "live-v1", id: "artifact-page", op: "query", target: host.identity, command: valid });
		const result = await client.next();
		assert.equal(result.ok, true);
		assert.equal(result.result.commandType, "artifact_history");
	}
	client.send({ protocol: "live-v1", id: "artifact-result", op: "get_result", target: host.identity, requestId: "artifact-page" });
	assert.equal((await client.next()).error.code, "result_not_found");
	const invalidCommands = [
		{ type: "artifact_history", cursor: { leafId: null } },
		{ type: "artifact_history", cursor: { leafId: null, before: -1 } },
		{ type: "artifact_history", cursor: { leafId: null, before: 0.5 } },
		{ type: "artifact_history", cursor: { leafId: "", before: 0 } },
		{ type: "artifact_history", cursor: { leafId: "bad\nleaf", before: 0 } },
		{ type: "artifact_history", cursor: { leafId: "x".repeat(513), before: 0 } },
		{ type: "artifact_history", cursor: { leafId: null, before: 0, extra: true } },
		{ type: "artifact_history", cursor: { leafId: null, before: 0 }, maxItems: 0 },
		{ type: "artifact_history", cursor: { leafId: null, before: 0 }, maxItems: 501 },
		{ type: "artifact_history", cursor: { leafId: null, before: 0 }, maxBytes: 1 },
		{ type: "artifact_history", cursor: { leafId: null, before: 0 }, maxBytes: 512 * 1024 + 1 },
		{ type: "artifact_history", cursor: { leafId: null, before: 0 }, unknown: true },
		{ type: "artifact_history", id: "native-id", cursor: { leafId: null, before: 0 } },
	];
	for (const [index, command] of invalidCommands.entries()) {
		client.send({ protocol: "live-v1", id: `invalid-artifacts-${index}`, op: "query", target: host.identity, command });
		assert.equal((await client.next()).error.code, "invalid_request");
	}
	assert.equal(host.dispatchCalls.length, 2);
});

test("canonical output references pass through snapshots, pages, retained results, events and replay without recursive inspection", async (t) => {
	const host = new FakeHost();
	const publicationId = randomUUID();
	const artifact = {
		publicationId, kind: "image", displayName: "output.png", mediaType: "image/png", byteLength: 3,
		sha256: "a".repeat(64), disposition: "inline", retention: "runtime_source", origin: "native",
		availability: { status: "available" },
	};
	const resolved = { type: "output_artifact_reference", kind: "image", state: "resolved", artifact };
	const missing = {
		type: "output_artifact_reference", kind: "image", state: "missing",
		source: { type: "tool_result_image", toolCallId: "tool-1", contentIndex: 1 },
		mediaType: "image/png", reason: "publication_missing",
	};
	const nestedImage = { type: "image", data: "nested-data-must-remain", mimeType: "image/png" };
	const toolResult = {
		role: "toolResult", toolCallId: "tool-1", toolName: "image-tool", timestamp: 1,
		content: [{ type: "text", text: "before" }, resolved, { type: "text", text: "after" }],
		details: { nested: nestedImage },
	};
	const page = {
		scope: "active_context", messages: [toolResult],
		coverage: { scope: "active_context", startIndex: 0, endIndexExclusive: 1, totalMessages: 1, byteLength: 256, hasOlder: false, hasNewer: false },
	};
	const branchItem = { itemId: "entry-1", entryId: "entry-1", type: "message", message: { ...toolResult, content: [missing] } };
	host.snapshotExtra = {
		...outputArtifactPresentationSnapshot(),
		messages: [toolResult],
		branchTranscript: [branchItem],
		streamingMessage: toolResult,
	};
	host.dispatchImpl = async (_target, command) => (command as { type: string }).type === "branch_history"
		? { scope: "current_branch", items: [branchItem], coverage: { scope: "current_branch", leafId: null, startIndex: 0, endIndexExclusive: 1, totalItems: 1, byteLength: 256, hasOlder: false, hasNewer: false } }
		: page;
	const { socketPath } = await fixture(t, {}, host);
	const current = await JsonlClient.connect(socketPath);
	t.after(() => current.close());
	current.send({ protocol: "live-v1", id: "output-sub", op: "subscribe" });
	assert.equal((await current.next()).result.mode, "snapshot");
	const snapshot = await current.next();
	assert.deepEqual(snapshot.snapshot.messages, [toolResult]);
	assert.deepEqual(snapshot.snapshot.branchTranscript, [branchItem]);
	assert.deepEqual(snapshot.snapshot.streamingMessage, toolResult);
	current.send({ protocol: "live-v1", id: "output-snapshot", op: "snapshot" });
	assert.deepEqual((await current.next()).result.snapshot.messages, [toolResult]);
	for (const [id, command, expected] of [
		["output-history", { type: "history", cursor: { leafId: null, before: 1 }, maxMessages: 1, maxBytes: 1024 }, page],
		["output-branch", { type: "branch_history", cursor: { leafId: null, before: 1 }, maxItems: 1, maxBytes: 1024 }, { scope: "current_branch", items: [branchItem], coverage: { scope: "current_branch", leafId: null, startIndex: 0, endIndexExclusive: 1, totalItems: 1, byteLength: 256, hasOlder: false, hasNewer: false } }],
	] as const) {
		current.send({ protocol: "live-v1", id, op: "query", target: host.identity, command });
		assert.deepEqual((await current.next()).result, expected);
	}
	current.send({
		protocol: "live-v1", id: "retained-output-history", op: "dispatch", target: host.identity,
		command: { type: "history", cursor: { leafId: null, before: 1 }, maxMessages: 1, maxBytes: 1024 },
	});
	const retained = await current.next();
	assert.deepEqual(retained.result.response, page);
	current.send({ protocol: "live-v1", id: "poll-output-history", op: "get_result", target: host.identity, requestId: "retained-output-history" });
	assert.deepEqual((await current.next()).result.response, page);
	const eventPayloads = [
		{ type: "tool_execution_update", partialResult: toolResult },
		{ type: "tool_execution_end", result: toolResult },
		{ type: "message_start", message: toolResult },
		{ type: "message_end", message: toolResult },
		{ type: "turn_end", toolResults: [toolResult] },
		{ type: "entry_appended", entry: branchItem },
	];
	const emitted = eventPayloads.map((event) => host.emit("session_event", { event }));
	for (const event of emitted) assert.deepEqual((await current.next()).event, event);
	const replay = await JsonlClient.connect(socketPath);
	t.after(() => replay.close());
	replay.send({ protocol: "live-v1", id: "output-replay", op: "subscribe", after: { streamId: host.identity.streamId, sequence: 0 } });
	assert.equal((await replay.next()).result.mode, "replay");
	for (const event of emitted) assert.deepEqual((await replay.next()).event, event);
	assert.equal(JSON.stringify(emitted).includes("nested-data-must-remain"), true);
	assert.equal(JSON.stringify(emitted).includes(publicationId), true);
});

test("input-image capability requires the complete native snapshot, query and copy surface", async (t) => {
	const complete = makeInputImageHost();
	const snapshotOnly = new FakeHost();
	snapshotOnly.snapshotExtra = inputImageSnapshot();
	const copyOnly = new FakeHost();
	Object.assign(copyOnly, { copyInputImageBytes: async () => ({ byteLength: 0, sha256: "0".repeat(64) }) });
	const lookupOnly = new FakeHost();
	Object.assign(lookupOnly, { getCurrentBranchInputImage: () => { throw new Error("missing"); } });
	const incompleteSnapshot = new FakeHost();
	incompleteSnapshot.snapshotExtra = inputImageSnapshot();
	delete incompleteSnapshot.snapshotExtra.pendingInputImages;
	addInputImageCopy(incompleteSnapshot);

	for (const [name, host, expected] of [
		["complete", complete, true],
		["legacy", new FakeHost(), false],
		["snapshot-only", snapshotOnly, false],
		["copy-only", copyOnly, false],
		["lookup-only", lookupOnly, false],
		["incomplete-snapshot", incompleteSnapshot, false],
	] as const) {
		const { socketPath } = await fixture(t, {}, host);
		const client = await JsonlClient.connect(socketPath);
		t.after(() => client.close());
		client.send({ protocol: "live-v1", id: `caps-${name}`, op: "capabilities" });
		const capabilities = (await client.next()).result;
		assert.equal(capabilities.queryCommandTypes.includes("input_image_history"), expected);
		assert.deepEqual(capabilities.inputImages, expected ? INPUT_IMAGE_CAPABILITY : undefined);
		assert.equal(capabilities.attachments, undefined);
		assert.equal(capabilities.managedArtifacts, undefined);
		client.send({
			protocol: "live-v1",
			id: `query-${name}`,
			op: "query",
			target: host.identity,
			command: { type: "input_image_history", cursor: { leafId: null, before: 0 }, maxItems: 1, maxBytes: 2 },
		});
		const query = await client.next();
		assert.equal(query.ok, expected);
		if (!expected) assert.equal(query.error.code, "invalid_request");
	}
	assert.equal(complete.dispatchCalls.length, 1);
	assert.equal(snapshotOnly.dispatchCalls.length, 0);
	assert.equal(copyOnly.dispatchCalls.length, 0);
	assert.equal(lookupOnly.dispatchCalls.length, 0);
	assert.equal(incompleteSnapshot.dispatchCalls.length, 0);
});

test("input_image_history is a strictly validated pure query", async (t) => {
	const host = makeInputImageHost();
	host.dispatchImpl = async (_target, command) => ({
		scope: "current_branch_input_images",
		inputImages: [],
		coverage: {
			scope: "current_branch_input_images", leafId: null, startIndex: 0, endIndexExclusive: 0,
			totalInputImages: 0, byteLength: 2, hasOlder: false, hasNewer: false,
		},
		commandType: (command as { type: string }).type,
	});
	const { socketPath } = await fixture(t, {}, host);
	const client = await JsonlClient.connect(socketPath);
	t.after(() => client.close());
	const valid = { type: "input_image_history", cursor: { leafId: null, before: 0 }, maxItems: 500, maxBytes: 512 * 1024 };
	client.send({ protocol: "live-v1", id: "input-images", op: "query", target: host.identity, command: valid });
	const result = await client.next();
	assert.equal(result.ok, true);
	assert.equal(result.result.commandType, "input_image_history");
	client.send({ protocol: "live-v1", id: "input-images-result", op: "get_result", target: host.identity, requestId: "input-images" });
	assert.equal((await client.next()).error.code, "result_not_found");

	const invalidCommands = [
		{ type: "input_image_history", cursor: { leafId: null } },
		{ type: "input_image_history", cursor: { leafId: null, before: -1 } },
		{ type: "input_image_history", cursor: { leafId: null, before: 0.5 } },
		{ type: "input_image_history", cursor: { leafId: null, before: 0, extra: true } },
		{ type: "input_image_history", cursor: { leafId: null, before: 0 }, maxItems: 0 },
		{ type: "input_image_history", cursor: { leafId: null, before: 0 }, maxItems: 501 },
		{ type: "input_image_history", cursor: { leafId: null, before: 0 }, maxBytes: 1 },
		{ type: "input_image_history", cursor: { leafId: null, before: 0 }, maxBytes: 512 * 1024 + 1 },
		{ type: "input_image_history", cursor: { leafId: null, before: 0 }, unknown: true },
		{ type: "input_image_history", id: "native-id", cursor: { leafId: null, before: 0 } },
	];
	for (const [index, command] of invalidCommands.entries()) {
		client.send({ protocol: "live-v1", id: `invalid-input-images-${index}`, op: "query", target: host.identity, command });
		assert.equal((await client.next()).error.code, "invalid_request");
	}
	assert.equal(host.dispatchCalls.length, 1);
});

test("projected input-image events retain native sequence and replay without socket rewriting", async (t) => {
	const host = makeInputImageHost();
	const { socketPath } = await fixture(t, {}, host);
	const current = await JsonlClient.connect(socketPath);
	t.after(() => current.close());
	current.send({ protocol: "live-v1", id: "input-sub", op: "subscribe" });
	assert.equal((await current.next()).result.mode, "snapshot");
	await current.next();

	const inputImage = {
		inputImageId: randomUUID(),
		mediaType: "image/png",
		byteLength: 3,
		sha256: "a".repeat(64),
		source: { type: "user_message_image", contentIndex: 1 },
		origin: "native",
		availability: { status: "available" },
	};
	const referenced = host.emit("input_image_referenced", { inputImage });
	const directFrame = await current.next();
	assert.deepEqual(directFrame.event, referenced);
	const nested = { type: "image", data: "not-socket-inspected", mimeType: "image/png" };
	const ordinary = host.emit("session_event", { event: { type: "custom", details: { nested } } });
	const ordinaryFrame = await current.next();
	assert.deepEqual(ordinaryFrame.event, ordinary);
	assert.deepEqual(ordinaryFrame.event.event.details.nested, nested);

	const replay = await JsonlClient.connect(socketPath);
	t.after(() => replay.close());
	replay.send({ protocol: "live-v1", id: "input-replay", op: "subscribe", after: { streamId: host.identity.streamId, sequence: 0 } });
	assert.equal((await replay.next()).result.mode, "replay");
	assert.deepEqual((await replay.next()).event, referenced);
	assert.deepEqual((await replay.next()).event, ordinary);
	assert.deepEqual([referenced.sequence, ordinary.sequence], [1, 2]);
});

test("pure native queries bypass mutation retention and reject effectful command types", async (t) => {
	const { socketPath, host } = await fixture(t, {
		maxResults: 1,
		maxResultBytes: 4096,
		maxConcurrentQueries: 1,
	});
	host.dispatchImpl = async (_target, command) => ({
		type: (command as { type: string }).type,
		value: "query-result",
	});
	const client = await JsonlClient.connect(socketPath);
	t.after(() => client.close());

	for (const command of [
		{ type: "history", cursor: { leafId: null, before: 0 }, maxMessages: 1, maxBytes: 2 },
		{ type: "branch_history", cursor: { leafId: null, before: 0 }, maxItems: 1, maxBytes: 2 },
		{ type: "get_commands" },
	]) {
		client.send({ protocol: "live-v1", id: "reusable-query", op: "query", target: host.identity, command });
		const reply = await client.next();
		assert.equal(reply.ok, true);
		assert.equal(reply.result.type, command.type);
	}
	assert.equal(host.dispatchCalls.length, 3);
	client.send({
		protocol: "live-v1",
		id: "query-poll",
		op: "get_result",
		target: host.identity,
		requestId: "reusable-query",
	});
	assert.equal((await client.next()).error.code, "result_not_found");

	const rejectedQueries = [
		{ type: "prompt", message: "no" },
		{ type: "bash", command: "true" },
		{ type: "set_model", provider: "p", modelId: "m" },
		{ type: "new_session" },
		{ type: "interactive_builtin", operationId: "op", command: "/new" },
		{ type: "interaction_response", interactionId: "i", response: { kind: "cancel" } },
		{ type: "interaction_update", interactionId: "i", expectedRevision: 0, action: { domain: "scoped_models", action: "persist" } },
	];
	for (const [index, command] of rejectedQueries.entries()) {
		client.send({ protocol: "live-v1", id: `bad-query-${index}`, op: "query", target: host.identity, command });
		assert.equal((await client.next()).error.code, "invalid_request");
	}
	assert.equal(host.dispatchCalls.length, 3);

	client.send({
		protocol: "live-v1",
		id: "mutation-after-queries",
		op: "dispatch",
		target: host.identity,
		command: { type: "abort", id: "native-mutation" },
	});
	assert.equal((await client.next()).result.status, "completed");
	assert.equal(host.dispatchCalls.length, 4);
});

test("mutation result byte budget retains completed-unavailable tombstones and known IDs", async (t) => {
	const { socketPath, host } = await fixture(t, {
		maxResults: 2,
		maxResultBytes: 8192,
	});
	host.dispatchImpl = async (_target, command) => {
		const id = (command as { id?: string }).id;
		if (id === "native-unserializable") {
			const cyclic: { self?: unknown } = {};
			cyclic.self = cyclic;
			return cyclic;
		}
		if (id === "native-budget") return { payload: "x".repeat(5000) };
		return { ok: true };
	};
	const client = await JsonlClient.connect(socketPath);
	t.after(() => client.close());

	client.send({
		protocol: "live-v1", id: "unserializable", op: "dispatch", target: host.identity,
		command: { type: "abort", id: "native-unserializable" },
	});
	const unserializable = await client.next();
	assert.equal(unserializable.result.status, "unavailable");
	assert.equal(unserializable.result.outcome, "completed");
	assert.equal(unserializable.result.reason, "response_unserializable");

	client.send({
		protocol: "live-v1", id: "budgeted", op: "dispatch", target: host.identity,
		command: { type: "abort", id: "native-budget" },
	});
	const budgeted = await client.next();
	assert.equal(budgeted.result.status, "unavailable");
	assert.equal(budgeted.result.outcome, "completed");
	assert.equal(budgeted.result.reason, "retention_budget");

	for (const requestId of ["unserializable", "budgeted"]) {
		client.send({ protocol: "live-v1", id: `poll-${requestId}`, op: "get_result", target: host.identity, requestId });
		assert.equal((await client.next()).result.status, "unavailable");
		client.send({
			protocol: "live-v1", id: requestId, op: "dispatch", target: host.identity,
			command: { type: "abort", id: `duplicate-${requestId}` },
		});
		assert.equal((await client.next()).error.code, "duplicate_request");
	}
	assert.equal(host.dispatchCalls.length, 2);
});

test("wire validation, local permissions, client capacity and cleanup are bounded", async (t) => {
	const { directory, socketPath, host, server } = await fixture(t, { maxClients: 1 });
	assert.equal((await fs.promises.stat(directory)).mode & 0o777, 0o700);
	assert.equal((await fs.promises.stat(socketPath)).mode & 0o777, 0o600);
	assert.equal(path.basename(liveSocketPath("session-1", directory)), "session-1.live.sock");
	assert.throws(() => liveSocketPath("../escape", directory), /sessionId/);

	const client = await JsonlClient.connect(socketPath);
	t.after(() => client.close());
	client.send({ protocol: "live-v1", id: "caps", op: "capabilities" });
	const caps = await client.next();
	assert.equal(caps.result.protocol, "live-v1");
	assert.equal(caps.result.remoteAuth, "external");
	assert.equal(caps.result.limits.maxClients, 1);
	assert.equal(caps.result.limits.maxInputBytes, 1024 * 1024);
	assert.equal(caps.result.limits.maxFrameBytes, 1024 * 1024);
	assert.equal(caps.result.limits.maxClientQueueBytes, 1024 * 1024);
	assert.equal(caps.result.limits.maxResultBytes, 8 * 1024 * 1024);
	assert.equal(caps.result.limits.maxConcurrentQueries, 16);
	assert.ok(caps.result.operations.includes("query"));
	assert.ok(caps.result.resultStates.includes("unavailable"));
	assert.ok(caps.result.queryCommandTypes.includes("history"));
	assert.ok(caps.result.queryCommandTypes.includes("branch_history"));
	assert.ok(caps.result.queryCommandTypes.includes("get_commands"));
	assert.ok(!caps.result.queryCommandTypes.includes("prompt"));
	assert.equal(caps.result.transcript.snapshot, "bounded-with-coverage");
	assert.deepEqual(caps.result.transcript.history, { direction: "older", maxMessages: 500, maxBytes: 768 * 1024 });
	assert.deepEqual(caps.result.transcript.branchHistory, { direction: "older", maxItems: 500, maxBytes: 768 * 1024 });

	client.send({ protocol: "live-v1", id: "bad", op: "snapshot", surprise: true });
	const invalid = await client.next();
	assert.equal(invalid.ok, false);
	assert.equal(invalid.error.code, "invalid_request");

	const excess = await JsonlClient.connect(socketPath);
	const capacity = await excess.next();
	assert.equal(capacity.error.code, "capacity");
	excess.close();

	const secondHost = new FakeHost();
	await assert.rejects(
		startLiveTransport(secondHost, { socketPath }),
		/already active/,
	);
	assert.equal(secondHost.closed, true);

	client.close();
	await server.close();
	assert.equal(host.closed, true);
	assert.equal(fs.existsSync(socketPath), false);
});

test("the normal extension factory capability-detects automatic live startup and cleans duplicate lifecycles", async (t) => {
	type ReadyHandler = (context: { liveRpcSession: LiveTransportHost }) => Promise<void | (() => void | Promise<void>)>;
	const registrations: ReadyHandler[] = [];
	const eventHandlers = new Map<string, Array<(...args: any[]) => unknown>>();
	const baseApi = {
		on(event: string, handler: (...args: any[]) => unknown) {
			const handlers = eventHandlers.get(event) ?? [];
			handlers.push(handler);
			eventHandlers.set(event, handlers);
		},
		getCommands: () => [],
		setModel: async () => true,
		getThinkingLevel: () => "off",
		setThinkingLevel: () => {},
		sendUserMessage: () => {},
	};
	assert.doesNotThrow(() => extensionFactory(baseApi as never));
	extensionFactory({
		...baseApi,
		registerInteractiveReady(handler: ReadyHandler) {
			registrations.push(handler);
		},
	} as never);
	assert.equal(registrations.length, 1);

	const transferKeys = ["SESH_PI_TRANSFER_THREAD_ID", "SESH_PI_TRANSFER_INBOX", "SESH_PI_TRANSFER_API_SOCKET"] as const;
	const previousTransfer = Object.fromEntries(transferKeys.map((key) => [key, process.env[key]]));
	const previousConsoleError = console.error;
	const reported: string[] = [];
	process.env.SESH_PI_TRANSFER_THREAD_ID = randomUUID();
	process.env.SESH_PI_TRANSFER_INBOX = "/tmp/pi-rpc-missing-helper-inbox";
	process.env.SESH_PI_TRANSFER_API_SOCKET = "/tmp/pi-rpc-missing-helper-api.sock";
	console.error = (...values: unknown[]) => reported.push(values.map(String).join(" "));
	const restoreTransferEnvironment = () => {
		for (const key of transferKeys) {
			const value = previousTransfer[key];
			if (value === undefined) delete process.env[key];
			else process.env[key] = value;
		}
		console.error = previousConsoleError;
	};
	t.after(restoreTransferEnvironment);

	const firstHost = new FakeHost();
	firstHost.identity.sessionId = `factory-${randomUUID()}`;
	const firstPath = liveSocketPath(firstHost.identity.sessionId, pickSocketsDir());
	const firstCleanup = await registrations[0]!({ liveRpcSession: firstHost });
	assert.equal(typeof firstCleanup, "function");
	assert.equal(fs.existsSync(firstPath), true);
	const client = await JsonlClient.connect(firstPath);
	client.send({ protocol: "live-v1", id: "factory-caps", op: "capabilities" });
	assert.equal((await client.next()).result.protocol, "live-v1");
	client.send({ protocol: "live-v1", id: "factory-attachment-caps", op: "capabilities" });
	assert.equal((await client.next()).result.attachments, undefined);
	assert.ok(reported.some((message) => message.includes("prepareRpcPromptFiles")));
	client.close();
	await assert.rejects(registrations[0]!({ liveRpcSession: new FakeHost() }), /already active/);
	await firstCleanup!();
	await firstCleanup!();
	assert.equal(firstHost.closed, true);
	assert.equal(fs.existsSync(firstPath), false);
	restoreTransferEnvironment();

	const replacementHost = new FakeHost();
	replacementHost.identity.sessionId = `factory-${randomUUID()}`;
	const replacementPath = liveSocketPath(replacementHost.identity.sessionId, pickSocketsDir());
	const replacementCleanup = await registrations[0]!({ liveRpcSession: replacementHost });
	assert.equal(fs.existsSync(replacementPath), true);
	await replacementCleanup!();
	assert.equal(replacementHost.closed, true);
	assert.equal(fs.existsSync(replacementPath), false);
});
