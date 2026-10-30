import { test, type TestContext } from "node:test";
import assert from "node:assert/strict";
import * as net from "node:net";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { randomUUID } from "node:crypto";
import { validateLiveWireCommand } from "../live-wire-validation.ts";
import {
	startLiveTransport,
	type LiveEvent,
	type LiveIdentity,
	type LiveSnapshot,
	type LiveTransportHost,
} from "../live-transport.ts";

/**
 * Extension-authored payloads that must never be retained by the transport. The native
 * contract keeps update results payload-free, so these appear only inside live interaction
 * events and the authoritative snapshot.
 */
const SENTINEL_TITLE = "EXT-TITLE-SENTINEL api.example.test/secret-path";
const SENTINEL_STATE = "EXT-STATE-SENTINEL token=abcd1234";
const SENTINELS = [SENTINEL_TITLE, SENTINEL_STATE] as const;

const assertNoSentinel = (value: unknown, label = "frame") => {
	const encoded = JSON.stringify(value) ?? "";
	for (const sentinel of SENTINELS) {
		assert.ok(!encoded.includes(sentinel), `${label} must not contain ${sentinel}: ${encoded}`);
	}
};

const extensionView = (revision: number) => ({
	interactionId: "11111111-2222-4333-8444-555555555555",
	revision,
	kind: "extension_view",
	title: SENTINEL_TITLE,
	browserAnswerable: true,
	viewId: "portable-session-model/selector",
	viewVersion: 1,
	state: { query: SENTINEL_STATE, items: [{ id: "m1", label: SENTINEL_STATE }] },
	hasTerminalPresentation: true,
	remoteCancellable: true,
});

/** The exact closed identity the native contract publishes on resolution. */
const resolvedExtensionView = (revision: number) => ({
	kind: "extension_view",
	interactionId: "11111111-2222-4333-8444-555555555555",
	revision,
	viewId: "portable-session-model/selector",
	viewVersion: 1,
});

const scopedModels = (revision: number) => ({
	interactionId: "99999999-8888-4777-8666-555555555555",
	revision,
	kind: "scoped_models",
	title: "Session models",
	browserAnswerable: true,
	state: { available: [], scoped: [] },
});

class InteractionHost implements LiveTransportHost {
	readonly identity: LiveIdentity = { streamId: "stream-ext", sessionEpoch: 1, sessionId: "session-ext" };
	sequence = 0;
	snapshotExtra: Record<string, unknown> = { pendingInteractions: [] };
	readonly dispatchCalls: unknown[] = [];
	readonly dispatchTargets: LiveIdentity[] = [];
	dispatchImpl: (command: unknown, output: (response: unknown) => void) => Promise<unknown | undefined> =
		async (_command, output) => {
			output({ status: "applied", revision: 1 });
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

	dispatch(target: LiveIdentity, command: unknown, output: (response: unknown) => void): Promise<unknown | undefined> {
		this.dispatchCalls.push(command);
		this.dispatchTargets.push(target);
		return this.dispatchImpl(command, output);
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

type Fixture = {
	socketPath: string;
	host: InteractionHost;
	errors: string[];
};

async function fixture(t: TestContext, host = new InteractionHost()): Promise<Fixture> {
	const directory = await fs.promises.mkdtemp(path.join(os.tmpdir(), "pi-ext-view-"));
	const socketPath = path.join(directory, "fixture.live.sock");
	const errors: string[] = [];
	const server = await startLiveTransport(host, {
		socketPath,
		onError: (error) => errors.push(error instanceof Error ? error.message : String(error)),
	});
	t.after(async () => {
		await server.close();
		await fs.promises.rm(directory, { recursive: true, force: true });
	});
	return { socketPath, host, errors };
}

const subscribeFresh = async (client: JsonlClient, id: string) => {
	client.send({ protocol: "live-v1", id, op: "subscribe" });
	const response = await client.next();
	const snapshot = await client.next();
	return { response, snapshot };
};

const subscribeAfter = async (client: JsonlClient, id: string, streamId: string, sequence: number) => {
	client.send({ protocol: "live-v1", id, op: "subscribe", after: { streamId, sequence } });
	return await client.next();
};

test("extension-view lifecycle events reach attached observers but are never replayed", async (t) => {
	const { socketPath, host, errors } = await fixture(t);
	const attached = await JsonlClient.connect(socketPath);
	t.after(() => attached.close());
	await subscribeFresh(attached, "ext-attached");

	const before = host.emit("runtime_state", { state: "ready-before" });
	const opened = host.emit("interaction_opened", { interaction: extensionView(0) });
	const between = host.emit("runtime_state", { state: "ready-between" });
	const updated = host.emit("interaction_updated", { interaction: extensionView(1), source: "remote" });
	const resolved = host.emit("interaction_resolved", {
		interaction: resolvedExtensionView(1),
		resolution: { source: "remote", kind: "completed" },
	});
	const after = host.emit("runtime_state", { state: "ready-after" });

	// Live delivery to an already-attached observer is unchanged, payload included.
	assert.deepEqual((await attached.next()).event, before);
	const liveOpened = await attached.next();
	assert.deepEqual(liveOpened.event, opened);
	assert.equal(liveOpened.event.interaction.title, SENTINEL_TITLE);
	assert.deepEqual((await attached.next()).event, between);
	assert.deepEqual((await attached.next()).event, updated);
	assert.deepEqual((await attached.next()).event, resolved);
	assert.deepEqual((await attached.next()).event, after);
	attached.close();

	// Every replay interval that would have to cross a barriered event degrades to gap
	// plus snapshot.
	for (const [label, sequence] of [
		["before the view opened", before.sequence],
		["between opened and updated", between.sequence],
		["after the final update", updated.sequence],
	] as const) {
		const late = await JsonlClient.connect(socketPath);
		t.after(() => late.close());
		const response = await subscribeAfter(late, `ext-late-${sequence}`, host.identity.streamId, sequence);
		assert.equal(response.result.mode, "snapshot", label);
		const gap = await late.next();
		assert.equal(gap.type, "gap", label);
		const snapshot = await late.next();
		assert.equal(snapshot.type, "snapshot", label);
		assert.equal(snapshot.snapshot.sequence, after.sequence, label);
		assertNoSentinel([response, gap, snapshot], label);
		await assert.rejects(late.next(250), /timed out/, `${label}: no barriered event is replayed`);
		late.close();
	}

	// Resuming exactly FROM the barrier floor is legitimate: the client already saw the
	// resolved event live, and only the trailing non-barriered event is retained.
	const tail = await JsonlClient.connect(socketPath);
	t.after(() => tail.close());
	const tailResponse = await subscribeAfter(tail, "ext-tail", host.identity.streamId, resolved.sequence);
	assert.equal(tailResponse.result.mode, "replay", "the resolved sequence is the barrier floor");
	const tailEvent = await tail.next();
	assert.deepEqual(tailEvent.event, after, "only the post-barrier event is replayed");
	assertNoSentinel([tailResponse, tailEvent]);
	await assert.rejects(tail.next(250), /timed out/, "no barriered event follows");
	assert.deepEqual(errors, []);
});

test("the authoritative snapshot remains the only post-reconnect source of view state", async (t) => {
	const host = new InteractionHost();
	host.snapshotExtra = { pendingInteractions: [extensionView(2)] };
	const { socketPath } = await fixture(t, host);
	const client = await JsonlClient.connect(socketPath);
	t.after(() => client.close());

	host.emit("interaction_opened", { interaction: extensionView(0) });
	const updated = host.emit("interaction_updated", { interaction: extensionView(2), source: "terminal" });

	const { response, snapshot } = await subscribeFresh(client, "ext-current");
	assert.equal(response.result.mode, "snapshot");
	assert.equal(snapshot.snapshot.pendingInteractions[0].title, SENTINEL_TITLE, "current state is served");
	assert.equal(snapshot.snapshot.pendingInteractions[0].state.query, SENTINEL_STATE);

	// After the view closes, the same reconnect recovers nothing.
	host.snapshotExtra = { pendingInteractions: [] };
	host.emit("interaction_resolved", {
		interaction: resolvedExtensionView(2),
		resolution: { source: "terminal", kind: "cancelled" },
	});
	const reconnect = await JsonlClient.connect(socketPath);
	t.after(() => reconnect.close());
	const reconnectResponse = await subscribeAfter(reconnect, "ext-reconnect", host.identity.streamId, updated.sequence);
	const reconnectGap = await reconnect.next();
	const reconnectSnapshot = await reconnect.next();
	assert.equal(reconnectGap.type, "gap");
	assertNoSentinel([reconnectResponse, reconnectGap, reconnectSnapshot], "post-resolution reconnect");
	assert.deepEqual(reconnectSnapshot.snapshot.pendingInteractions, []);
});

test("the barrier is exactly kind-scoped and leaves other interaction kinds replayable", async (t) => {
	const { socketPath, host, errors } = await fixture(t);
	const first = host.emit("interaction_opened", { interaction: scopedModels(0) });
	const second = host.emit("interaction_updated", { interaction: scopedModels(1), source: "remote" });
	const third = host.emit("interaction_resolved", {
		interaction: scopedModels(1),
		resolution: { source: "remote", kind: "completed" },
	});
	// Lifecycle events for other kinds, and malformed lifecycle events without an exact
	// extension_view interaction, keep the existing retention behaviour.
	const confirmOpened = host.emit("interaction_opened", {
		interaction: { interactionId: randomUUID(), kind: "confirm", title: "Proceed?", message: "ok" },
	});
	const missingInteraction = host.emit("interaction_updated", { source: "remote" });
	const nullInteraction = host.emit("interaction_opened", { interaction: null });
	const otherKindString = host.emit("interaction_opened", { interaction: { kind: "extension_view_like" } });

	const client = await JsonlClient.connect(socketPath);
	t.after(() => client.close());
	const response = await subscribeAfter(client, "kinds", host.identity.streamId, first.sequence - 1);
	assert.equal(response.result.mode, "replay", "no barrier was raised by other kinds");
	for (const event of [first, second, third, confirmOpened, missingInteraction, nullInteraction, otherKindString]) {
		assert.deepEqual((await client.next()).event, event);
	}
	assert.deepEqual(errors, []);
});

test("a payload-free update result stays payload-free through the retained ledger", async (t) => {
	const { socketPath, host, errors } = await fixture(t);
	const client = await JsonlClient.connect(socketPath);
	t.after(() => client.close());

	// Exactly the native wire contract: no interaction, state, title, result or action.
	const results = [
		{ status: "applied", revision: 3 },
		{ status: "stale", revision: 4 },
		{ status: "settled" },
		{ status: "already_resolved" },
		{ status: "not_found" },
		{ status: "invalid", error: "Extension view action was refused" },
	];
	for (const [index, hostResult] of results.entries()) {
		host.dispatchImpl = async (_command, output) => {
			output(hostResult);
			return undefined;
		};
		const requestId = randomUUID();
		client.send({
			protocol: "live-v1",
			id: requestId,
			op: "dispatch",
			target: host.identity,
			command: {
				type: "interaction_update",
				interactionId: "11111111-2222-4333-8444-555555555555",
				expectedRevision: index,
				// The current wire admits only the scoped-models action domain; see the
				// extension_view admission test below.
				action: { domain: "scoped_models", action: "toggle", modelId: "m1" },
			},
		});
		const response = await client.next();
		assert.equal(response.ok, true, hostResult.status);
		assert.equal(response.result.status, "completed", hostResult.status);
		assert.deepEqual(response.result.response, hostResult, hostResult.status);

		client.send({ protocol: "live-v1", id: `get-${requestId}`, op: "get_result", target: host.identity, requestId });
		const retained = await client.next();
		assert.deepEqual(retained.result.response, hostResult, hostResult.status);
		assertNoSentinel([response, retained], `retained ${hostResult.status} result`);

		// Target and UUID deduplication are unchanged for this command.
		client.send({
			protocol: "live-v1",
			id: requestId,
			op: "dispatch",
			target: host.identity,
			command: { type: "interaction_update", interactionId: "11111111-2222-4333-8444-555555555555", expectedRevision: index, action: { domain: "scoped_models", action: "clear" } },
		});
		assert.equal((await client.next()).error.code, "duplicate_request", hostResult.status);
		client.send({
			protocol: "live-v1",
			id: `mismatch-${requestId}`,
			op: "get_result",
			target: { ...host.identity, sessionEpoch: host.identity.sessionEpoch + 1 },
			requestId,
		});
		assert.equal((await client.next()).error.code, "result_target_mismatch", hostResult.status);
	}
	assert.equal(host.dispatchCalls.length, results.length, "one dispatch per accepted request");
	assert.deepEqual(errors, []);
});

test("an unserializable retained event reports fixed diagnostic text", async (t) => {
	const { socketPath, host, errors } = await fixture(t);
	const attached = await JsonlClient.connect(socketPath);
	t.after(() => attached.close());
	await subscribeFresh(attached, "serial-attached");
	const kept = host.emit("runtime_state", { state: "ready-before-cycle" });
	assert.deepEqual((await attached.next()).event, kept);
	attached.close();

	// A retained (non-barriered) event the transport cannot serialize must not echo the
	// thrown exception, which names property paths such as `secretField`.
	const cyclic: Record<string, unknown> = { note: SENTINEL_TITLE };
	cyclic.secretField = { [SENTINEL_STATE]: cyclic };
	const event = host.emit("runtime_state", { state: "cyclic", detail: cyclic });
	assert.equal(
		errors[0],
		`Live event runtime_state at sequence ${event.sequence} could not be serialized for replay`,
	);
	// A still-attached observer's delivery attempt may add the fixed encode message; both
	// are fixed strings, and no diagnostic may name a property path or payload.
	assert.deepEqual(
		[...new Set(errors)].filter((message) => message !== "response is not serializable"),
		[`Live event runtime_state at sequence ${event.sequence} could not be serialized for replay`],
		errors.join("\n"),
	);
	for (const message of errors) {
		for (const forbidden of ["secretField", "circular", "constructor", "-->", SENTINEL_TITLE, SENTINEL_STATE]) {
			assert.ok(!message.includes(forbidden), `diagnostic must not contain ${forbidden}: ${message}`);
		}
	}

	// The unserializable event is also a replay barrier, so nothing before it survives.
	const late = await JsonlClient.connect(socketPath);
	t.after(() => late.close());
	const response = await subscribeAfter(late, "serial-late", host.identity.streamId, kept.sequence);
	assert.equal(response.result.mode, "snapshot");
	assert.equal((await late.next()).type, "gap");
	assertNoSentinel([response, await late.next()], "post-barrier snapshot");
});

test("an unserializable extension-view event is barriered before any serialization", async (t) => {
	const { socketPath, host, errors } = await fixture(t);
	const attached = await JsonlClient.connect(socketPath);
	t.after(() => attached.close());
	await subscribeFresh(attached, "ext-cyclic-attached");
	const kept = host.emit("runtime_state", { state: "ready-before-ext-cycle" });
	assert.deepEqual((await attached.next()).event, kept);

	const cyclic: Record<string, unknown> = { kind: "extension_view", title: SENTINEL_TITLE };
	cyclic.secretField = { [SENTINEL_STATE]: cyclic };
	const event = host.emit("interaction_updated", { interaction: cyclic, source: "remote" });

	// The kind check precedes serialization, so the replay diagnostic never runs; only the
	// live delivery attempt fails, with fixed prose and no property path.
	assert.ok(!errors.some((message) => message.includes("could not be serialized for replay")), errors.join("\n"));
	assert.ok(errors.length >= 1, "the delivery failure is reported");
	for (const message of errors) {
		for (const forbidden of ["secretField", "circular", "constructor", "-->", SENTINEL_TITLE, SENTINEL_STATE]) {
			assert.ok(!message.includes(forbidden), `diagnostic must not contain ${forbidden}: ${message}`);
		}
	}

	const late = await JsonlClient.connect(socketPath);
	t.after(() => late.close());
	const response = await subscribeAfter(late, "ext-cyclic-late", host.identity.streamId, kept.sequence);
	assert.equal(response.result.mode, "snapshot", "the barrier floor advanced past the event");
	assert.equal((await late.next()).type, "gap");
	const snapshot = await late.next();
	assert.ok(event.sequence > kept.sequence);
	assertNoSentinel([response, snapshot], "post-barrier snapshot");
});

const VIEW_ID = "portable-session-model/selector";
const INTERACTION_ID = "11111111-2222-4333-8444-555555555555";

const extensionAction = (action: unknown) => ({ domain: "extension_view", viewId: VIEW_ID, action });

const updateCommand = (action: unknown, expectedRevision = 0) => ({
	type: "interaction_update",
	interactionId: INTERACTION_ID,
	expectedRevision,
	action,
});

/** Call the wire validator directly for values a JSON frame cannot carry. */
const wireRefusal = (command: Record<string, unknown>): string => {
	try {
		validateLiveWireCommand(command, (message) => {
			throw new Error(message);
		});
	} catch (error) {
		return error instanceof Error ? error.message : String(error);
	}
	throw new Error("expected the wire validator to refuse");
};

/**
 * `levels` nested containers wrapping a scalar. The action root is depth 0, so a scalar
 * under `levels` containers sits at depth `levels`; eight containers is the maximum.
 */
const nestObjects = (levels: number): unknown => {
	let value: unknown = 1;
	for (let level = 0; level < levels; level += 1) value = { next: value };
	return value;
};

const nestArrays = (levels: number): unknown => {
	let value: unknown = 1;
	for (let level = 0; level < levels; level += 1) value = [value];
	return value;
};

/** Alternating object/array containers, `levels` deep. */
const nestMixed = (levels: number): unknown => {
	let value: unknown = "leaf";
	for (let level = 0; level < levels; level += 1) value = level % 2 === 0 ? [value] : { next: value };
	return value;
};

test("a bounded extension_view action is dispatched and retained payload-free", async (t) => {
	const { socketPath, host, errors } = await fixture(t);
	const client = await JsonlClient.connect(socketPath);
	t.after(() => client.close());
	host.dispatchImpl = async (_command, output) => {
		output({ status: "applied", revision: 4 });
		return undefined;
	};

	const action = extensionAction({
		kind: "choose",
		id: "anthropic/claude-sonnet-4-5",
		query: "sonnet",
		scope: { enabled: true, order: ["a", "b"], note: null, weight: -1.5 },
	});
	const requestId = randomUUID();
	client.send({ protocol: "live-v1", id: requestId, op: "dispatch", target: host.identity, command: updateCommand(action, 3) });
	const response = await client.next();
	assert.equal(response.ok, true, JSON.stringify(response));
	assert.equal(response.result.status, "completed");
	assert.deepEqual(response.result.response, { status: "applied", revision: 4 });

	// The socket forwards the command unchanged and interprets no action meaning.
	assert.equal(host.dispatchCalls.length, 1);
	assert.deepEqual(host.dispatchCalls[0], updateCommand(action, 3));

	client.send({ protocol: "live-v1", id: `get-${requestId}`, op: "get_result", target: host.identity, requestId });
	const retained = await client.next();
	assert.deepEqual(retained.result.response, { status: "applied", revision: 4 });
	assertNoSentinel([response, retained], "extension action dispatch");

	// Outer exact-target and UUID guards are unchanged for this command.
	client.send({ protocol: "live-v1", id: requestId, op: "dispatch", target: host.identity, command: updateCommand(action, 3) });
	assert.equal((await client.next()).error.code, "duplicate_request");
	client.send({
		protocol: "live-v1",
		id: `mismatch-${requestId}`,
		op: "get_result",
		target: { ...host.identity, sessionEpoch: host.identity.sessionEpoch + 1 },
		requestId,
	});
	assert.equal((await client.next()).error.code, "result_target_mismatch");
	assert.equal(host.dispatchCalls.length, 1, "a duplicate request never reaches the host again");
	// Target authority stays native's: the socket forwards the original target unchanged.
	assert.deepEqual(host.dispatchTargets[0], host.identity);

	// Values at the declared edges are accepted: depth 8, a 64-character key, a bare
	// JSON scalar action, and an empty object.
	for (const [label, accepted] of [
		["maximum container depth", nestObjects(8)],
		["maximum key length", { ["k".repeat(64)]: "v" }],
		["bare scalar", "choose"],
		["empty object", {}],
		["empty array", []],
		["nested arrays", [[["a"], 1], null, true]],
	] as const) {
		const id = randomUUID();
		client.send({
			protocol: "live-v1",
			id,
			op: "dispatch",
			target: host.identity,
			command: updateCommand(extensionAction(accepted)),
		});
		const accept = await client.next();
		assert.equal(accept.ok, true, `${label}: ${JSON.stringify(accept)}`);
		assert.equal(accept.result.status, "completed", label);
	}
	assert.deepEqual(errors, []);
});

test("malformed and oversized extension_view actions are refused before the host", async (t) => {
	const { socketPath, host, errors } = await fixture(t);
	const client = await JsonlClient.connect(socketPath);
	t.after(() => client.close());

	const refusals: Array<[string, unknown]> = [
		["missing viewId", { domain: "extension_view", action: { kind: "choose" } }],
		["missing action", { domain: "extension_view", viewId: VIEW_ID }],
		["extra field", { ...extensionAction({}), viewVersion: 1 }],
		["empty viewId", { domain: "extension_view", viewId: "", action: {} }],
		["upper-case viewId", { domain: "extension_view", viewId: "Portable/Selector", action: {} }],
		["leading dash viewId", { domain: "extension_view", viewId: "-portable/selector", action: {} }],
		["dotted viewId", { domain: "extension_view", viewId: "portable.selector", action: {} }],
		["spaced viewId", { domain: "extension_view", viewId: "portable selector", action: {} }],
		["long viewId", { domain: "extension_view", viewId: `a${"b".repeat(128)}`, action: {} }],
		["non-string viewId", { domain: "extension_view", viewId: 7, action: {} }],
		["prototype pollution key", extensionAction(JSON.parse('{"__proto__":{"polluted":true}}'))],
		["constructor key", extensionAction({ constructor: "x" })],
		["prototype key", extensionAction({ prototype: "x" })],
		["too deep", extensionAction(nestObjects(9))],
		["long key", extensionAction({ ["k".repeat(65)]: "v" })],
		["oversized string", extensionAction({ blob: "x".repeat(8193) })],
		["oversized payload", extensionAction({ rows: Array.from({ length: 900 }, () => "0123456789") })],
		["oversized wide object", extensionAction(Object.fromEntries(Array.from({ length: 3000 }, (_value, index) => [`k${index}`, index])))],
	];

	for (const [label, action] of refusals) {
		const requestId = randomUUID();
		client.send({ protocol: "live-v1", id: requestId, op: "dispatch", target: host.identity, command: updateCommand(action) });
		const refused = await client.next();
		assert.equal(refused.ok, false, label);
		assert.equal(refused.error.code, "invalid_request", label);
		// Fixed diagnostics only: no offending key, value, size or payload text.
		for (const forbidden of ["polluted", "0123456789", "k2999", "x".repeat(32), "k".repeat(65), "__proto__"]) {
			assert.ok(!refused.error.message.includes(forbidden), `${label}: ${refused.error.message}`);
		}
		client.send({ protocol: "live-v1", id: `get-${requestId}`, op: "get_result", target: host.identity, requestId });
		assert.equal((await client.next()).error.code, "result_not_found", `${label}: no identity was reserved`);
	}
	assert.equal(host.dispatchCalls.length, 0, "no refused action reached the host");
	assert.deepEqual(errors, []);
});

test("the action boundary accepts only plain JSON data", () => {
	const plainRefusals: Array<[string, unknown]> = [
		["function", () => undefined],
		["undefined", undefined],
		["NaN", Number.NaN],
		["Infinity", Number.POSITIVE_INFINITY],
		["bigint", BigInt(1)],
		["Date", new Date()],
		["Map", new Map()],
		["RegExp", /x/],
		["class instance", new (class Holder { value = 1; })()],
	];
	for (const [label, action] of plainRefusals) {
		assert.equal(wireRefusal(updateCommand(extensionAction(action))), "command.action must be plain JSON data", label);
		assert.equal(wireRefusal(updateCommand(extensionAction({ nested: action }))), "command.action must be plain JSON data", `nested ${label}`);
	}

	const getter = {};
	Object.defineProperty(getter, "secret", { enumerable: true, get: () => "SECRET" });
	assert.equal(wireRefusal(updateCommand(extensionAction(getter))), "command.action must be plain JSON data", "accessor property");

	const symbolKeyed: Record<string, unknown> = { ok: 1 };
	(symbolKeyed as Record<symbol, unknown>)[Symbol("secret")] = 1;
	assert.equal(wireRefusal(updateCommand(extensionAction(symbolKeyed))), "command.action must be plain JSON data", "symbol key");

	const holes = [1];
	holes[3] = 2;
	assert.equal(wireRefusal(updateCommand(extensionAction(holes))), "command.action must be plain JSON data", "array hole");

	const cyclic: Record<string, unknown> = { kind: "choose" };
	cyclic.self = cyclic;
	assert.equal(wireRefusal(updateCommand(extensionAction(cyclic))), "command.action must be plain JSON data", "cycle");
	const cyclicArray: unknown[] = [];
	cyclicArray.push(cyclicArray);
	assert.equal(wireRefusal(updateCommand(extensionAction(cyclicArray))), "command.action must be plain JSON data", "array cycle");

	assert.equal(
		wireRefusal(updateCommand(extensionAction(nestObjects(9)))),
		"command.action exceeds the extension view action bounds",
		"depth",
	);

	// A shared object alias is refused exactly as a cycle is, matching Pi's own rule.
	// Parsed wire JSON cannot contain an alias, so this is observable only here.
	const shared = { id: "m1" };
	assert.equal(
		wireRefusal(updateCommand(extensionAction({ a: shared, b: shared }))),
		"command.action must be plain JSON data",
		"shared object alias",
	);
	const sharedArray: unknown[] = [];
	assert.equal(
		wireRefusal(updateCommand(extensionAction({ a: sharedArray, b: sharedArray }))),
		"command.action must be plain JSON data",
		"shared array alias",
	);
	// Equal-but-distinct values are ordinary data and are accepted.
	validateLiveWireCommand(updateCommand(extensionAction({ a: { id: "m1" }, b: { id: "m1" } })), (message) => {
		throw new Error(message);
	});
	// A null-prototype object carries no inherited behaviour and is plain data.
	const bare = Object.create(null) as Record<string, unknown>;
	bare.kind = "choose";
	validateLiveWireCommand(updateCommand(extensionAction(bare)), (message) => {
		throw new Error(message);
	});
});

test("the depth bound permits exactly eight nested containers", async (t) => {
	const { socketPath, host, errors } = await fixture(t);
	const client = await JsonlClient.connect(socketPath);
	t.after(() => client.close());

	const accept = (action: unknown): void => {
		validateLiveWireCommand(updateCommand(extensionAction(action)), (message) => {
			throw new Error(message);
		});
	};
	const refuse = (action: unknown, label: string): void => {
		assert.equal(
			wireRefusal(updateCommand(extensionAction(action))),
			"command.action exceeds the extension view action bounds",
			label,
		);
	};

	// A scalar action is depth 0; each container adds one level; eight containers wrapping
	// a scalar is the documented maximum, and the ninth is refused.
	accept(1);
	for (const [label, build] of [
		["objects", nestObjects],
		["arrays", nestArrays],
		["mixed", nestMixed],
	] as const) {
		for (let levels = 1; levels <= 8; levels += 1) accept(build(levels));
		refuse(build(9), `nine ${label}`);
		refuse(build(12), `twelve ${label}`);
	}

	// An empty container may itself sit at the deepest permitted position.
	accept(nestObjects(7));
	accept((() => {
		let value: unknown = {};
		for (let level = 0; level < 8; level += 1) value = { next: value };
		return value;
	})());

	// Depth is per path, not cumulative: many eight-deep siblings stay acceptable.
	accept({ a: nestObjects(7), b: nestArrays(7), c: nestMixed(7) });

	// The same edges hold over the real wire, refused before the host.
	const deepest = randomUUID();
	client.send({
		protocol: "live-v1",
		id: deepest,
		op: "dispatch",
		target: host.identity,
		command: updateCommand(extensionAction(nestObjects(8))),
	});
	assert.equal((await client.next()).result.status, "completed", "eight containers are dispatched");
	assert.equal(host.dispatchCalls.length, 1);

	const tooDeep = randomUUID();
	client.send({
		protocol: "live-v1",
		id: tooDeep,
		op: "dispatch",
		target: host.identity,
		command: updateCommand(extensionAction(nestObjects(9))),
	});
	const refused = await client.next();
	assert.equal(refused.error.code, "invalid_request");
	client.send({ protocol: "live-v1", id: `get-${tooDeep}`, op: "get_result", target: host.identity, requestId: tooDeep });
	assert.equal((await client.next()).error.code, "result_not_found");
	assert.equal(host.dispatchCalls.length, 1, "the deep action never reached the host");
	assert.deepEqual(errors, []);
});

test("byte accounting matches exact JSON serialization at the 8 KiB edge", () => {
	const accept = (action: unknown): void => {
		validateLiveWireCommand(updateCommand(extensionAction(action)), (message) => {
			throw new Error(message);
		});
	};
	const refuse = (action: unknown, label: string): void => {
		assert.equal(
			wireRefusal(updateCommand(extensionAction(action))),
			"command.action exceeds the extension view action bounds",
			label,
		);
	};
	const serializedBytes = (action: unknown) => Buffer.byteLength(JSON.stringify(action));
	const LIMIT = 8 * 1024;

	// One long string in an object: `{"blob":"<n chars>"}` serializes to n + 11 bytes.
	const blob = (chars: number) => ({ blob: "x".repeat(chars) });
	assert.equal(serializedBytes(blob(LIMIT - 11)), LIMIT, "fixture is exactly at the limit");
	accept(blob(LIMIT - 11));
	assert.equal(serializedBytes(blob(LIMIT - 10)), LIMIT + 1);
	refuse(blob(LIMIT - 10), "one byte over with a string");

	// For each shape, the largest size whose exact serialization fits is accepted and the
	// next size is refused, so the socket's incremental total tracks JSON.stringify.
	const assertBoundary = (build: (size: number) => unknown, label: string): void => {
		let size = 1;
		while (serializedBytes(build(size)) <= LIMIT) size += 1;
		const largest = build(size - 1);
		const smallestOver = build(size);
		assert.ok(serializedBytes(largest) <= LIMIT, label);
		assert.ok(serializedBytes(smallestOver) > LIMIT, label);
		accept(largest);
		refuse(smallestOver, label);
	};

	// A flat array of single-digit numbers: `[` + n digits + (n-1) commas + `]` = 2n + 1,
	// so no size lands exactly on 8192; the pair either side of the limit is asserted.
	assertBoundary((count) => Array.from({ length: count }, () => 7), "array of digits");
	// Many short keys: each `"kNN":v` pair plus its comma is accounted exactly.
	assertBoundary(
		(count) => Object.fromEntries(Array.from({ length: count }, (_value, index) => [`k${index}`, 1])),
		"wide object",
	);
	// Nested containers and every scalar form measured together.
	assertBoundary((pad) => ({ a: [true, false, null, -1.5, 1e21], b: { c: "y".repeat(pad) } }), "mixed scalars");
	// Deeply nested within the depth bound.
	assertBoundary((pad) => ({ a: { b: { c: { d: { e: { f: { g: "z".repeat(pad) } } } } } } }), "deep nesting");

	// The budget is bytes, not characters: multi-byte text costs its UTF-8 length even
	// though its character count is under the separate string-length bound.
	const accented = { blob: "é".repeat(4100) };
	assert.ok(JSON.stringify(accented.blob).length - 2 < 8192, "character count is under the string bound");
	assert.ok(serializedBytes(accented) > LIMIT, "but its UTF-8 size is over the byte bound");
	refuse(accented, "multi-byte string");
	const accentedFits = { blob: "é".repeat(4000) };
	assert.ok(serializedBytes(accentedFits) <= LIMIT);
	accept(accentedFits);

	// Keys are measured in bytes too, including their quotes and colon.
	const unicodeKey = { ["ü".repeat(32)]: "v" };
	assert.ok(serializedBytes(unicodeKey) <= LIMIT);
	accept(unicodeKey);
	refuse({ ["ü".repeat(65)]: "v" }, "key over 64 characters");
});

test("existing interaction update domains are unchanged", () => {
	const accepted: unknown[] = [
		{ domain: "scoped_models", action: "toggle", modelId: "m1" },
		{ domain: "scoped_models", action: "enable", query: "son" },
		{ domain: "scoped_models", action: "clear" },
		{ domain: "scoped_models", action: "toggle_provider", provider: "anthropic" },
		{ domain: "scoped_models", action: "move", modelId: "m1", direction: "up" },
		{ domain: "scoped_models", action: "persist" },
		{ domain: "model_thinking", action: "set", modelId: "m1", level: "high" },
		{ domain: "model_thinking", action: "clear", modelId: "m1" },
		{ domain: "theme", action: "apply_single", theme: "dark" },
		{ domain: "theme", action: "apply_automatic", lightTheme: "light", darkTheme: "dark" },
	];
	for (const action of accepted) {
		validateLiveWireCommand(updateCommand(action), (message) => {
			throw new Error(`${message}: ${JSON.stringify(action)}`);
		});
	}
	// A string action name is still required outside the extension-view domain.
	assert.equal(
		wireRefusal(updateCommand({ domain: "scoped_models", action: { kind: "toggle" } })),
		"command.action.action must be text",
	);
	assert.equal(wireRefusal(updateCommand({ domain: "scoped_models", action: "unknown_action" })),
		"command.action.action has an unsupported scoped_models value");
	assert.equal(wireRefusal(updateCommand({ domain: "unknown_domain", action: "x" })),
		"command.action.domain has an unsupported value");
	assert.equal(wireRefusal({ type: "interaction_update", interactionId: INTERACTION_ID, action: extensionAction({}) }),
		"missing command field: expectedRevision");
});

test("a client reply for an unserializable value carries fixed prose only", async (t) => {
	const host = new InteractionHost();
	const cyclic: Record<string, unknown> = { pendingInteractions: [] };
	cyclic.secretField = { [SENTINEL_STATE]: cyclic };
	host.snapshotExtra = cyclic;
	const { socketPath } = await fixture(t, host);
	const client = await JsonlClient.connect(socketPath);
	t.after(() => client.close());
	client.send({ protocol: "live-v1", id: "snap-cyclic", op: "snapshot" });
	const response = await client.next();
	assert.equal(response.ok, false);
	assert.equal(response.error.code, "internal_error");
	assert.equal(response.error.message, "response is not serializable");
	for (const forbidden of ["secretField", "circular", "constructor", "-->", SENTINEL_STATE]) {
		assert.ok(!JSON.stringify(response).includes(forbidden), `reply must not contain ${forbidden}`);
	}
});
