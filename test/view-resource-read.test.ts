import { test, type TestContext } from "node:test";
import assert from "node:assert/strict";
import * as net from "node:net";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import {
	startLiveTransport,
	detectLiveViewResourceCapability,
	type LiveEvent,
	type LiveIdentity,
	type LiveSnapshot,
	type LiveTransportHost,
} from "../live-transport.ts";
import extensionFactory from "../index.ts";

/**
 * `view_resource_read`: one bounded page of one view's resource, as a pure query.
 *
 * The contract these tests hold the socket to is the five-member native union —
 * `page | stale | not_found | already_resolved | invalid` — forwarded verbatim, with
 * no sixth status and no re-classification. `read_timed_out` is NOT a status: the host
 * returns `{status:"invalid", error:"…read deadline."}`, so the `error` prose is the
 * only thing that distinguishes a retryable timeout from a terminal rejection, and it
 * must survive the hop untouched.
 */

/**
 * Copied field for field from the host's own LIVE_RPC_VIEW_RESOURCE_CAPABILITY
 * (`packages/coding-agent/src/modes/rpc/live-rpc-session.ts`). An exact-keys check
 * against a fixture that does not match the real producer passes every test while the
 * command never enters the allowlist in production, so this constant is only
 * meaningful if it is the host's record and not the test's idea of it.
 */
const CAPABILITY = {
	protocol: "pi.view-resources",
	version: 1,
	read: "view-resource-read-v1",
	staleness: "binding-stale-v1",
	retention: "requester-only",
} as const;

const REF = {
	interactionId: "int-1",
	viewId: "demo/output",
	viewVersion: 1,
	expectedRevision: 7,
	datasetId: "leaf:abc",
} as const;

const read = (overrides: Record<string, unknown> = {}, id = "vr-1") => ({
	protocol: "live-v1",
	id,
	op: "query",
	target: { streamId: "stream-1", sessionEpoch: 0, sessionId: "session-1" },
	command: {
		type: "view_resource_read",
		interactionId: REF.interactionId,
		ref: { ...REF },
		resource: "output",
		cursor: { entryId: "e1", line: 7, segment: 3 },
		...overrides,
	},
});

const PAGE = {
	status: "page",
	page: {
		resource: "output",
		binding: { viewId: REF.viewId, viewVersion: 1, revision: 7, datasetId: REF.datasetId },
		items: [{ entryId: "e1", text: "hello" }],
		coverage: { from: 0, to: 1, total: 1 },
	},
} as const;

class ViewHost implements LiveTransportHost {
	readonly identity: LiveIdentity = { streamId: "stream-1", sessionEpoch: 0, sessionId: "session-1" };
	sequence = 0;
	closed = false;
	dispatched: unknown[] = [];
	snapshotExtra: Record<string, unknown> = { viewResourceCapability: { ...CAPABILITY } };
	result: unknown = PAGE;
	resultFor?: (command: Record<string, unknown>) => unknown;
	hold?: Promise<void>;
	private listeners = new Set<(event: LiveEvent) => void>();

	snapshot(): LiveSnapshot {
		return {
			protocol: "live-v1", streamId: this.identity.streamId, sessionEpoch: this.identity.sessionEpoch,
			sessionId: this.identity.sessionId, watermark: this.sequence, state: {},
			...this.snapshotExtra,
		} as unknown as LiveSnapshot;
	}
	subscribe(listener: (event: LiveEvent) => void): () => void {
		this.listeners.add(listener);
		return () => this.listeners.delete(listener);
	}
	emit(event: LiveEvent): void {
		for (const listener of this.listeners) listener(event);
	}
	async dispatch(_target: LiveIdentity, command: unknown): Promise<unknown> {
		this.dispatched.push(command);
		if (this.hold) await this.hold;
		return this.resultFor ? this.resultFor(command as Record<string, unknown>) : this.result;
	}
	close(): void {
		this.closed = true;
	}
}

class Client {
	private frames: any[] = [];
	private waiters: Array<() => void> = [];
	private buffer = "";
	private constructor(private socket: net.Socket) {
		socket.setEncoding("utf8");
		socket.on("data", (chunk: string) => {
			this.buffer += chunk;
			let index = this.buffer.indexOf("\n");
			while (index >= 0) {
				const line = this.buffer.slice(0, index);
				this.buffer = this.buffer.slice(index + 1);
				if (line.trim()) this.frames.push(JSON.parse(line));
				index = this.buffer.indexOf("\n");
			}
			for (const waiter of this.waiters.splice(0)) waiter();
		});
	}
	static async connect(socketPath: string): Promise<Client> {
		const socket = net.createConnection(socketPath);
		await new Promise<void>((resolve, reject) => {
			socket.once("connect", resolve);
			socket.once("error", reject);
		});
		return new Client(socket);
	}
	send(value: unknown): void {
		this.socket.write(JSON.stringify(value) + "\n");
	}
	async next(timeoutMs = 2000): Promise<any> {
		const deadline = Date.now() + timeoutMs;
		while (this.frames.length === 0) {
			const remaining = deadline - Date.now();
			if (remaining <= 0) throw new Error("timed out waiting for a frame");
			await new Promise<void>((resolve, reject) => {
				const timer = setTimeout(() => reject(new Error("timed out waiting for a frame")), remaining);
				this.waiters.push(() => { clearTimeout(timer); resolve(); });
			});
		}
		return this.frames.shift();
	}
	/** Drain whatever has already arrived, without waiting for more. */
	drain(): any[] {
		return this.frames.splice(0);
	}
	close(): void {
		this.socket.destroy();
	}
}

async function fixture(t: TestContext, host = new ViewHost()) {
	const directory = await fs.promises.mkdtemp(path.join(os.tmpdir(), "pi-view-resource-"));
	const socketPath = path.join(directory, "fixture.live.sock");
	const server = await startLiveTransport(host, { socketPath, onError: () => {} });
	t.after(async () => {
		await server.close();
		await fs.promises.rm(directory, { recursive: true, force: true });
	});
	return { socketPath, host, server };
}

/**
 * Connect. The socket pushes NOTHING on attach — a snapshot arrives only in answer to
 * an explicit `subscribe` — so there is no startup frame to skip.
 */
async function connected(socketPath: string): Promise<Client> {
	return Client.connect(socketPath);
}

/**
 * A refusal. The socket answers on a `response` frame with `ok:false` and a coded
 * `error` — there is no separate `type:"error"` frame, so that is what to assert.
 */
function refused(frame: any, code?: string): void {
	assert.equal(frame.type, "response", JSON.stringify(frame));
	assert.equal(frame.ok, false, JSON.stringify(frame));
	if (code) assert.equal(frame.error.code, code, JSON.stringify(frame));
}

/** A success. */
function accepted(frame: any): void {
	assert.equal(frame.type, "response", JSON.stringify(frame));
	assert.equal(frame.ok, true, JSON.stringify(frame));
}

/** Subscribe and return the snapshot frame, which is what carries replay. */
async function subscribed(client: Client, id: string): Promise<any> {
	client.send({ protocol: "live-v1", id, op: "subscribe" });
	const ack = await client.next();
	assert.equal(ack.result.mode, "snapshot", JSON.stringify(ack));
	return client.next();
}

// 1 --------------------------------------------------------------------------
test("1: the capability is mirrored exactly, or not at all", async () => {
	// The key set is pinned, not just the behaviour. An exact-keys detector whose
	// expected fields drift from the producer's returns undefined forever and the
	// command never enters the allowlist -- while every other test here still passes,
	// because they all inject this same fixture. So assert the shape itself.
	assert.deepEqual(
		Object.keys(CAPABILITY).sort(),
		["protocol", "read", "retention", "staleness", "version"],
	);
	assert.equal(CAPABILITY.retention, "requester-only");
	assert.deepEqual(detectLiveViewResourceCapability({ ...CAPABILITY }), { ...CAPABILITY });
	assert.equal(Object.isFrozen(detectLiveViewResourceCapability({ ...CAPABILITY })), true);
	// A record the socket must NOT accept: extra key, missing key, wrong literal,
	// wrong version, or a near-miss contract name.
	for (const bad of [
		{ ...CAPABILITY, extra: 1 },
		{ protocol: "pi.view-resources", version: 1, read: "view-resource-read-v1", staleness: "binding-stale-v1" },
		{ ...CAPABILITY, retention: "published" },
		{ ...CAPABILITY, page: "bounded-32k-v1" },  // the field the socket must NOT expect
		{ ...CAPABILITY, version: 2 },
		{ ...CAPABILITY, protocol: "pi.view-resource" },
		{ ...CAPABILITY, read: "view-resource-read-v2" },
		null, undefined, "pi.view-resources", 1, [],
	]) {
		assert.equal(detectLiveViewResourceCapability(bad), undefined);
	}
});

// 2 --------------------------------------------------------------------------
test("2: without the capability the command is not in the allowlist and is refused", async (t) => {
	const host = new ViewHost();
	host.snapshotExtra = {}; // no capability on the startup snapshot
	const { socketPath } = await fixture(t, host);
	const client = await connected(socketPath);
	t.after(() => client.close());

	client.send({ protocol: "live-v1", id: "caps", op: "capabilities" });
	const caps = await client.next();
	assert.equal(caps.result.viewResources, undefined);
	assert.equal(caps.result.queryCommandTypes.includes("view_resource_read"), false);

	client.send(read());
	refused(await client.next(), "invalid_request");
	// Refused at the wire, so the host is never asked.
	assert.equal(host.dispatched.length, 0);
});

// 3 --------------------------------------------------------------------------
test("3: with the capability, a page round-trips verbatim", async (t) => {
	const { socketPath, host } = await fixture(t);
	const client = await connected(socketPath);
	t.after(() => client.close());

	client.send({ protocol: "live-v1", id: "caps", op: "capabilities" });
	const caps = await client.next();
	assert.deepEqual(caps.result.viewResources, { ...CAPABILITY });
	assert.equal(caps.result.queryCommandTypes.includes("view_resource_read"), true);

	client.send(read());
	const reply = await client.next();
	accepted(reply);
	assert.equal(reply.id, "vr-1");
	assert.deepEqual(reply.result, PAGE);
	// The command reached the host unmodified, including the cursor.
	assert.deepEqual(host.dispatched[0], (read().command as Record<string, unknown>));
});

// 4 --------------------------------------------------------------------------
test("4: a read retains nothing — no result record, no event, no replay, no snapshot page", async (t) => {
	const { socketPath, host } = await fixture(t);
	const client = await connected(socketPath);
	t.after(() => client.close());

	client.send(read());
	const reply = await client.next();
	assert.equal(reply.result.status, "page");

	// `get_result` must not know this id: a query is never retained.
	client.send({
		protocol: "live-v1", id: "gr-1", op: "get_result", requestId: "vr-1",
		target: { streamId: "stream-1", sessionEpoch: 0, sessionId: "session-1" },
	});
	const retained = await client.next();
	const state = retained.ok === false ? "refused" : retained.result?.state ?? retained.result?.status;
	assert.ok(
		state === "refused" || state === "unknown" || state === "expired",
		`expected unknown-or-expired, got ${JSON.stringify(retained)}`,
	);

	// No event frame was emitted for the read, so nothing entered the replay ring:
	// a fresh SUBSCRIBER replays nothing mentioning the resource.
	const observer = await Client.connect(socketPath);
	t.after(() => observer.close());
	const snapshotFrame = await subscribed(observer, "obs-1");
	const serialisedSnapshot = JSON.stringify(snapshotFrame);
	assert.equal(serialisedSnapshot.includes("view_resource_read"), false);
	assert.equal(serialisedSnapshot.includes("hello"), false);
	// The snapshot carries no page and no resource rows.
	assert.equal(snapshotFrame.snapshot?.page, undefined);
	// And nothing arrived unprompted on either connection.
	assert.deepEqual(client.drain(), []);
	assert.deepEqual(observer.drain(), []);

	// The id is reusable precisely because nothing was kept.
	host.result = { status: "not_found" };
	client.send(read({}, "vr-1"));
	const second = await client.next();
	assert.deepEqual(second.result, { status: "not_found" });
});

// 5 --------------------------------------------------------------------------
test("5: two independent viewers read different entryIds without interfering", async (t) => {
	const host = new ViewHost();
	host.resultFor = (command) => {
		const cursor = command.cursor as { entryId: string };
		return {
			status: "page",
			page: {
				resource: "output",
				binding: { viewId: REF.viewId, viewVersion: 1, revision: 7, datasetId: REF.datasetId },
				items: [{ entryId: cursor.entryId, text: `body-${cursor.entryId}` }],
				coverage: { from: 0, to: 1, total: 2 },
			},
		};
	};
	const { socketPath } = await fixture(t, host);
	const a = await connected(socketPath);
	const b = await connected(socketPath);
	t.after(() => { a.close(); b.close(); });

	// Same request id on both connections: ids are per-connection correlation, so
	// this must NOT collide.
	a.send(read({ cursor: { entryId: "alpha" } }, "same-id"));
	b.send(read({ cursor: { entryId: "beta" } }, "same-id"));
	const [ra, rb] = await Promise.all([a.next(), b.next()]);

	assert.equal(ra.result.page.items[0].entryId, "alpha");
	assert.equal(rb.result.page.items[0].entryId, "beta");
	assert.equal(ra.result.page.items[0].text, "body-alpha");
	assert.equal(rb.result.page.items[0].text, "body-beta");
	// Neither viewer saw the other's page.
	assert.deepEqual(a.drain(), []);
	assert.deepEqual(b.drain(), []);
});

// 6 --------------------------------------------------------------------------
test("6: all five statuses forward verbatim, and the invalid error prose survives", async (t) => {
	const { socketPath, host } = await fixture(t);
	const client = await connected(socketPath);
	t.after(() => client.close());

	const timedOut = "The view resource handler did not finish within the read deadline.";
	const cases: unknown[] = [
		PAGE,
		{ status: "stale", revision: 9, datasetId: "leaf:def" },
		{ status: "not_found" },
		{ status: "already_resolved" },
		{ status: "invalid", error: timedOut },
		{ status: "invalid", error: "The read did not match this view." },
	];
	for (const [index, expected] of cases.entries()) {
		host.result = expected;
		client.send(read({}, `vr-${index}`));
		const reply = await client.next();
		accepted(reply);
		assert.deepEqual(reply.result, expected);
	}
	// The whole point of C1: a timed-out read is distinguishable from a malformed one
	// ONLY by the error prose, so the socket must not normalise or drop it.
	host.result = { status: "invalid", error: timedOut };
	client.send(read({}, "vr-timeout"));
	const timeout = await client.next();
	assert.equal(timeout.result.status, "invalid");
	assert.equal(timeout.result.error, timedOut);
});

// 7 --------------------------------------------------------------------------
test("7: identity is never partial, and one view cannot borrow another's", async (t) => {
	const { socketPath, host } = await fixture(t);
	const client = await connected(socketPath);
	t.after(() => client.close());

	const bad: Array<[string, Record<string, unknown>]> = [
		["interactionId mismatch", { interactionId: "other" }],
		["missing viewId", { ref: { ...REF, viewId: undefined } }],
		["missing datasetId", { ref: { interactionId: REF.interactionId, viewId: REF.viewId, viewVersion: 1, expectedRevision: 7 } }],
		["empty datasetId", { ref: { ...REF, datasetId: "" } }],
		["unknown ref field", { ref: { ...REF, extra: 1 } }],
		["negative revision", { ref: { ...REF, expectedRevision: -1 } }],
		["non-integer viewVersion", { ref: { ...REF, viewVersion: 1.5 } }],
		["uppercase viewId", { ref: { ...REF, viewId: "Demo/Output" } }],
		["resource with an uppercase letter", { resource: "Output" }],
		["resource with a slash", { resource: "tool/index" }],
		["empty resource", { resource: "" }],
		["resource starting with an underscore", { resource: "_index" }],
		["resource over 64 chars", { resource: "x".repeat(65) }],
		["unknown command field", { nope: 1 }],
		["maxBytes above the ceiling", { maxBytes: 32 * 1024 + 1 }],
		["maxBytes zero", { maxBytes: 0 }],
		["cursor with a pollution key", { cursor: JSON.parse('{"__proto__":{"x":1}}') }],
		["cursor too deep", { cursor: JSON.parse("[".repeat(9) + "1" + "]".repeat(9)) }],
	];
	for (const [label, overrides] of bad) {
		client.send(read(overrides, "bad"));
		const reply = await client.next();
		assert.equal(reply.ok, false, `${label} should be refused: ${JSON.stringify(reply)}`);
		assert.equal(reply.error.code, "invalid_request", label);
	}
	// Every one was refused at the wire: the host was never asked.
	assert.equal(host.dispatched.length, 0);

	// The resource name is an OPAQUE identity the extension chose, so any well-formed
	// name is accepted and an unknown one is the host's call. `index` is the name the
	// only example view actually declares, and the first version of this wire refused
	// it while accepting that view's other resource, `output` — so a consumer testing
	// one resource would have concluded the wire was sound.
	for (const resource of ["index", "output", "input", "diagnostics", "tool_pane_index", "a-b_c", "x".repeat(64)]) {
		client.send(read({ resource }, "name-ok"));
		accepted(await client.next());
	}

	// maxBytes exactly at the ceiling is accepted...
	client.send(read({ maxBytes: 32 * 1024 }, "at-ceiling"));
	accepted(await client.next());
	// ...and one byte over is REFUSED, not clamped. Pinned because the first version
	// of this wire's own documentation said "clamped down", which is a different
	// consumer contract: a caller that asked for more would have been handed a smaller
	// page it could not tell apart from a short final page.
	client.send(read({ maxBytes: 32 * 1024 + 1 }, "over-ceiling"));
	const over = await client.next();
	assert.equal(over.ok, false);
	assert.equal(over.error.code, "invalid_request");
	assert.match(over.error.message, /maxBytes must be an integer from 1 to 32768/);
});

// 8 --------------------------------------------------------------------------
test("8: a malformed native result fails closed and is never forwarded as a page", async (t) => {
	const { socketPath, host } = await fixture(t);
	const client = await connected(socketPath);
	t.after(() => client.close());

	const malformed: unknown[] = [
		{ status: "unknown_status" },
		{ status: "page" },                                  // no page
		{ status: "page", page: "rows" },                    // page not an object
		{ status: "page", page: {}, extra: 1 },              // unknown sibling
		{ status: "stale" },                                 // missing revision/datasetId
		{ status: "stale", revision: -1, datasetId: "d" },   // negative revision
		{ status: "invalid" },                               // no error prose
		{ status: "invalid", error: 7 },                     // error not text
		{ status: "not_found", page: PAGE.page },            // rows on a non-page status
		null, undefined, "page", 42, [],
	];
	for (const value of malformed) {
		host.result = value;
		client.send(read({}, "mal"));
		const reply = await client.next();
		assert.equal(reply.ok, false, `should fail closed: ${JSON.stringify(value)}`);
		assert.equal(reply.error.code, "internal_error", JSON.stringify(reply));
		assert.equal(JSON.stringify(reply).includes("hello"), false, "no page bytes may leak");
	}
});

// 9 --------------------------------------------------------------------------
test("9: concurrency is bounded by maxConcurrentQueries and a duplicate id is refused", async (t) => {
	const host = new ViewHost();
	let release = () => {};
	host.hold = new Promise<void>((resolve) => { release = resolve; });
	const { socketPath } = await fixture(t, host);
	const client = await connected(socketPath);
	t.after(() => { release(); client.close(); });

	// 16 in flight is the documented bound; the 17th is refused with `capacity`.
	for (let index = 0; index < 16; index++) client.send(read({}, `vr-${index}`));
	client.send(read({}, "vr-16"));
	refused(await client.next(), "capacity");

	// A duplicate of an id already in flight is refused too, without disturbing it.
	client.send(read({}, "vr-0"));
	refused(await client.next(), "duplicate_request");

	release();
	// All 16 still settle.
	const settled = new Set<string>();
	while (settled.size < 16) {
		const frame = await client.next(4000);
		if (frame.type === "response" && frame.ok === true) settled.add(frame.id);
	}
	assert.equal(settled.size, 16);
});

// 10 -------------------------------------------------------------------------
test("10: a malformed target is refused at the wire; a STALE one is the host's call", async (t) => {
	const { socketPath, host } = await fixture(t);
	const client = await connected(socketPath);
	t.after(() => client.close());

	// Shape is the socket's business, and it refuses before any host call.
	for (const target of [
		{ streamId: "stream-1", sessionEpoch: 0 },                                  // missing member
		{ streamId: "stream-1", sessionEpoch: 0, sessionId: "s", extra: 1 },        // unknown member
		{ streamId: "stream-1", sessionEpoch: -1, sessionId: "s" },                 // negative epoch
		{ streamId: "", sessionEpoch: 0, sessionId: "s" },                          // empty id
		"stream-1",
	]) {
		client.send({ ...read({}, "bad-target"), target });
		const reply = await client.next();
		assert.equal(reply.ok, false, JSON.stringify(reply));
		assert.equal(reply.error.code, "invalid_request");
	}
	assert.equal(host.dispatched.length, 0, "a malformed target never reaches the host");

	// STALENESS, though, is the HOST's authority and not the socket's. `parseIdentity`
	// validates the target's SHAPE and nothing else — it never compares it to the
	// socket's own identity — so a well-formed but stale target IS dispatched, and the
	// host's assertion is what refuses it.
	host.dispatch = async () => {
		throw new Error("Live RPC target is stale; reconcile the session before submitting");
	};
	client.send({
		...read({}, "stale-target"),
		target: { streamId: "stream-1", sessionEpoch: 9, sessionId: "session-1" },
	});
	const stale = await client.next();
	assert.equal(stale.ok, false, JSON.stringify(stale));
	assert.equal(stale.error.code, "stale_target");
	// NOTE an asymmetry worth knowing rather than assuming away: a QUERY refusal goes
	// through `safeError`, which classifies the code by pattern but keeps the HOST's
	// own message (bounded to 512 chars). The socket-owned replacement text in
	// STRICT_DISPATCH_REFUSALS applies to the strict DISPATCH path only. So do not read
	// the dispatch guarantee "no native prose reaches a frame" as covering reads.
	assert.equal(stale.error.message, "Live RPC target is stale; reconcile the session before submitting");
	assert.ok(stale.error.message.length <= 512);
	// Either way, no page and no rows are forwarded.
	assert.equal(stale.result, undefined);
	assert.equal(JSON.stringify(stale).includes("hello"), false);
});

// 11 -------------------------------------------------------------------------
test("11: a genuine extension read end-to-end through the real socket", async (t) => {
	// The extension factory is driven exactly as a host drives it: no hand-written
	// frame, no direct startLiveTransport call. This is the normal-extension joined
	// path — registrar -> factory -> live transport -> real unix socket -> client.
	const hooks = new Map<string, Function>();
	const host = new ViewHost();
	let ready: ((context: { liveRpcSession: unknown }) => unknown) | undefined;
	const pi = {
		on: (event: string, fn: Function) => { hooks.set(event, fn); },
		registerInteractiveReady: (handler: (context: { liveRpcSession: unknown }) => unknown) => {
			ready = handler;
		},
		getCommands: () => [],
		getThinkingLevel: () => "off",
		getSessionName: () => undefined,
		sendUserMessage: () => {},
	} as unknown as Parameters<typeof extensionFactory>[0];

	extensionFactory(pi);
	assert.ok(ready, "the factory must register an interactive-ready handler");

	const cleanup = await ready!({ liveRpcSession: host });
	t.after(async () => {
		if (typeof cleanup === "function") await (cleanup as () => Promise<void>)();
	});

	// The socket path is the one the extension chose for this session id.
	const { liveSocketPath, pickSocketsDir } = await import("../socket-paths.ts");
	const socketPath = liveSocketPath(host.identity.sessionId, pickSocketsDir());
	for (let attempt = 0; attempt < 100 && !fs.existsSync(socketPath); attempt++) {
		await new Promise((resolve) => setTimeout(resolve, 10));
	}
	assert.equal(fs.existsSync(socketPath), true, `extension did not bind ${socketPath}`);

	const client = await connected(socketPath);
	t.after(() => client.close());

	client.send({ protocol: "live-v1", id: "caps", op: "capabilities" });
	const caps = await client.next();
	assert.deepEqual(caps.result.viewResources, { ...CAPABILITY });
	assert.equal(caps.result.queryCommandTypes.includes("view_resource_read"), true);

	client.send(read({}, "e2e-1"));
	const reply = await client.next();
	accepted(reply);
	assert.deepEqual(reply.result, PAGE);
	assert.equal((host.dispatched[0] as Record<string, unknown>).type, "view_resource_read");

	// Retention holds on this path too, not only against a directly-started transport.
	client.send({
		protocol: "live-v1", id: "e2e-gr", op: "get_result", requestId: "e2e-1",
		target: { ...host.identity },
	});
	const retained = await client.next();
	const state = retained.ok === false ? "refused" : retained.result?.state ?? retained.result?.status;
	assert.ok(state === "refused" || state === "unknown" || state === "expired", JSON.stringify(retained));
});
