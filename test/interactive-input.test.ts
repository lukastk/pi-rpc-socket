import { test, type TestContext } from "node:test";
import assert from "node:assert/strict";
import * as net from "node:net";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { validateLiveWireCommand } from "../live-wire-validation.ts";
import {
	detectLiveInteractiveInputCapability,
	startLiveTransport,
	type LiveEvent,
	type LiveIdentity,
	type LiveInteractiveInputCapability,
	type LiveSnapshot,
	type LiveTransportHost,
} from "../live-transport.ts";

/** Pi's exact six-field interactive-input capability, mirrored for fixtures only. */
const INTERACTIVE_INPUT_CAPABILITY: LiveInteractiveInputCapability = {
	protocol: "pi.interactive-input",
	version: 1,
	input: "interactive-input-v1",
	literal: "expand-prompt-templates-v1",
	busy: "steer-default-v1",
	bash: "not-remote",
};

/**
 * Lines whose SHAPE invites parsing. The socket must forward every one of them
 * unchanged: it never looks for a leading "/" or "!", and never decides whether a
 * line is a command.
 */
const SLASH_LINE = "/model anthropic/claude-opus-5";
const UNKNOWN_SLASH_LINE = "/not-a-real-command --flag value";
const BANG_LINE = "!rm -rf /tmp/should-never-run";
const TEMPLATE_LINE = "/skill:review file.ts";

class InputHost implements LiveTransportHost {
	readonly identity: LiveIdentity = {
		streamId: "stream-input",
		sessionEpoch: 0,
		sessionId: "session-input",
	};
	sequence = 0;
	snapshotExtra: Record<string, unknown> = {};
	readonly dispatchCalls: unknown[] = [];
	dispatchImpl: (command: unknown, output: (response: unknown) => void) => Promise<unknown | undefined> =
		async () => ({ disposition: "started" });
	private readonly listeners = new Set<(event: LiveEvent) => void>();

	snapshot(): LiveSnapshot {
		return { ...this.identity, sequence: this.sequence, ...this.snapshotExtra } as LiveSnapshot;
	}

	subscribe(listener: (event: LiveEvent) => void): () => void {
		this.listeners.add(listener);
		return () => this.listeners.delete(listener);
	}

	dispatch(_target: LiveIdentity, command: unknown, output: (response: unknown) => void): Promise<unknown | undefined> {
		this.dispatchCalls.push(command);
		return this.dispatchImpl(command, output);
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

async function fixture(t: TestContext, host = new InputHost()) {
	const directory = await fs.promises.mkdtemp(path.join(os.tmpdir(), "pi-input-"));
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

const capabilitiesOf = async (client: JsonlClient, id: string) => {
	client.send({ protocol: "live-v1", id, op: "capabilities" });
	return await client.next();
};

const dispatch = (command: Record<string, unknown>, id: string, host: InputHost) => ({
	protocol: "live-v1",
	id,
	op: "dispatch",
	target: { ...host.identity },
	command,
});

/** Collect the wire-validation message for a command, or null when it is accepted. */
const refusal = (command: Record<string, unknown>): string | null => {
	try {
		validateLiveWireCommand(command as any, (message) => {
			throw new Error(message);
		});
		return null;
	} catch (error) {
		return error instanceof Error ? error.message : String(error);
	}
};

// ---------------------------------------------------------------- capability

test("the interactive-input capability is detected only in its exact six-field form", () => {
	assert.deepEqual(
		detectLiveInteractiveInputCapability({ ...INTERACTIVE_INPUT_CAPABILITY }),
		INTERACTIVE_INPUT_CAPABILITY,
	);
	assert.deepEqual(
		detectLiveInteractiveInputCapability({
			bash: "not-remote",
			busy: "steer-default-v1",
			literal: "expand-prompt-templates-v1",
			input: "interactive-input-v1",
			version: 1,
			protocol: "pi.interactive-input",
		}),
		INTERACTIVE_INPUT_CAPABILITY,
		"object key order is irrelevant",
	);

	const mutable: Record<string, unknown> = { ...INTERACTIVE_INPUT_CAPABILITY };
	const detached = detectLiveInteractiveInputCapability(mutable)!;
	assert.equal(Object.isFrozen(detached), true);
	mutable.protocol = "pi.other";
	mutable.bash = "remote";
	assert.deepEqual(detached, INTERACTIVE_INPUT_CAPABILITY, "the detector returns a detached value");

	for (const field of Object.keys(INTERACTIVE_INPUT_CAPABILITY)) {
		const missing: Record<string, unknown> = { ...INTERACTIVE_INPUT_CAPABILITY };
		delete missing[field];
		assert.equal(detectLiveInteractiveInputCapability(missing), undefined, `missing ${field}`);
		assert.equal(
			detectLiveInteractiveInputCapability({ ...INTERACTIVE_INPUT_CAPABILITY, [field]: "wrong" }),
			undefined,
			`wrong ${field}`,
		);
	}
	assert.equal(detectLiveInteractiveInputCapability({ ...INTERACTIVE_INPUT_CAPABILITY, version: 2 }), undefined);
	assert.equal(detectLiveInteractiveInputCapability({ ...INTERACTIVE_INPUT_CAPABILITY, version: "1" }), undefined);
	assert.equal(detectLiveInteractiveInputCapability({ ...INTERACTIVE_INPUT_CAPABILITY, extra: true }), undefined);
	// A neighbouring capability must never be mistaken for this one.
	assert.equal(
		detectLiveInteractiveInputCapability({
			protocol: "pi.image-admission",
			version: 1,
			preparation: "rpc-prompt-files-strict-v1",
			dispatch: "standard-user-strict-v1",
			requestValidation: "normalized-messages-v1",
			virtualModels: "stable-profile-v1",
			binaryInLive: false,
		}),
		undefined,
	);
	for (const value of [undefined, null, 1, "pi.interactive-input", [], () => {}]) {
		assert.equal(detectLiveInteractiveInputCapability(value), undefined);
	}
});

test("capabilities mirrors the host record exactly, and omits it when absent", async (t) => {
	const host = new InputHost();
	host.snapshotExtra = { interactiveInputCapability: { ...INTERACTIVE_INPUT_CAPABILITY } };
	const { socketPath } = await fixture(t, host);
	const client = await JsonlClient.connect(socketPath);
	t.after(() => client.close());

	const reply = await capabilitiesOf(client, "caps-present");
	assert.equal(reply.ok, true);
	assert.deepEqual(reply.result.interactiveInput, INTERACTIVE_INPUT_CAPABILITY);
	// A read capability is a QUERY type; interactive_input is a mutation and must
	// never appear in the query command set.
	assert.ok(
		!reply.result.queryCommandTypes.includes("interactive_input"),
		"interactive_input is a mutation, not a query command",
	);

	const bare = new InputHost();
	const second = await fixture(t, bare);
	const other = await JsonlClient.connect(second.socketPath);
	t.after(() => other.close());
	const absent = await capabilitiesOf(other, "caps-absent");
	assert.equal("interactiveInput" in absent.result, false, "absence is legitimate, not an empty record");
});

test("a malformed host capability is treated as absent, never partially mirrored", async (t) => {
	const host = new InputHost();
	host.snapshotExtra = {
		interactiveInputCapability: { ...INTERACTIVE_INPUT_CAPABILITY, bash: "remote", extra: 1 },
	};
	const { socketPath } = await fixture(t, host);
	const client = await JsonlClient.connect(socketPath);
	t.after(() => client.close());
	const reply = await capabilitiesOf(client, "caps-malformed");
	assert.equal("interactiveInput" in reply.result, false);
});

// ---------------------------------------------------------------- validation

test("interactive_input accepts exactly its documented fields", () => {
	assert.equal(refusal({ type: "interactive_input", text: "hello" }), null, "text alone is enough");
	assert.equal(
		refusal({
			type: "interactive_input",
			id: "native-1",
			text: SLASH_LINE,
			busy: "followUp",
			literal: true,
			images: [{ type: "image", data: "AAA=", mimeType: "image/png" }],
		}),
		null,
		"every optional field together",
	);
	for (const busy of ["steer", "followUp"]) {
		assert.equal(refusal({ type: "interactive_input", text: "x", busy }), null, busy);
	}

	// Missing, wrong-typed and unknown fields are refused.
	assert.match(refusal({ type: "interactive_input" }) ?? "", /text/);
	assert.match(refusal({ type: "interactive_input", text: 7 }) ?? "", /text/);
	assert.match(refusal({ type: "interactive_input", text: "x", literal: "true" }) ?? "", /literal/);
	assert.match(refusal({ type: "interactive_input", text: "x", busy: "steer-default-v1" }) ?? "", /busy/);
	assert.match(refusal({ type: "interactive_input", text: "x", busy: "" }) ?? "", /busy/);
	assert.match(refusal({ type: "interactive_input", text: "x", unknown: 1 }) ?? "", /unknown/);
	assert.match(refusal({ type: "interactive_input", text: "x", message: "x" }) ?? "", /message/);
	assert.match(
		refusal({ type: "interactive_input", text: "x", images: [{ type: "image", data: "AAA=" }] }) ?? "",
		/images/,
	);
});

test("prompt carries expandPromptTemplates as a typed pass-through", () => {
	assert.equal(refusal({ type: "prompt", message: SLASH_LINE, expandPromptTemplates: false }), null);
	assert.equal(refusal({ type: "prompt", message: "hi", expandPromptTemplates: true }), null);
	assert.equal(refusal({ type: "prompt", message: "hi" }), null, "absent is still valid");
	assert.equal(
		refusal({
			type: "prompt",
			message: "hi",
			expandPromptTemplates: false,
			streamingBehavior: "steer",
			images: [{ type: "image", data: "AAA=", mimeType: "image/png" }],
		}),
		null,
		"composes with the existing optional fields",
	);
	assert.match(refusal({ type: "prompt", message: "hi", expandPromptTemplates: "false" }) ?? "", /expandPromptTemplates/);
	assert.match(refusal({ type: "prompt", message: "hi", expandPromptTemplates: 0 }) ?? "", /expandPromptTemplates/);
	// The option belongs to prompt only; it is not silently accepted elsewhere.
	assert.match(refusal({ type: "steer", message: "hi", expandPromptTemplates: false }) ?? "", /expandPromptTemplates/);
});

// ---------------------------------------------------------------- forwarding

test("the socket forwards an input line verbatim and never parses it", async (t) => {
	const host = new InputHost();
	host.snapshotExtra = { interactiveInputCapability: { ...INTERACTIVE_INPUT_CAPABILITY } };
	const { socketPath } = await fixture(t, host);
	const client = await JsonlClient.connect(socketPath);
	t.after(() => client.close());

	const lines = [SLASH_LINE, UNKNOWN_SLASH_LINE, BANG_LINE, TEMPLATE_LINE, "plain text", "   ", "/"];
	for (const [index, text] of lines.entries()) {
		const command = { type: "interactive_input", id: `native-${index}`, text };
		client.send(dispatch(command, `wire-${index}`, host));
		const reply = await client.next();
		assert.equal(reply.ok, true, `${text} dispatched`);
		assert.deepEqual(
			host.dispatchCalls[index],
			command,
			"the command reaches the host byte-for-byte: no trimming, no default busy, no literal flag",
		);
	}
	assert.deepEqual(host.dispatchCalls.length, lines.length);
});

test("the host's disposition is returned unchanged, including refused", async (t) => {
	const host = new InputHost();
	const dispositions = [
		{ disposition: "builtin_completed" },
		{ disposition: "builtin_cancelled" },
		{ disposition: "handled" },
		{ disposition: "started" },
		{ disposition: "queued" },
		{ disposition: "refused", reason: "empty" },
	];
	let next = 0;
	host.dispatchImpl = async () => dispositions[next++];
	const { socketPath } = await fixture(t, host);
	const client = await JsonlClient.connect(socketPath);
	t.after(() => client.close());

	for (const [index, expected] of dispositions.entries()) {
		client.send(dispatch({ type: "interactive_input", text: `line ${index}` }, `d-${index}`, host));
		const reply = await client.next();
		assert.equal(reply.ok, true);
		assert.deepEqual(reply.result.response, expected, "no status is rewritten or re-classified");
		assert.equal(reply.result.status, "completed");
	}
});

test("interactive_input is refused as a query and refused before the host when malformed", async (t) => {
	const host = new InputHost();
	const { socketPath } = await fixture(t, host);
	const client = await JsonlClient.connect(socketPath);
	t.after(() => client.close());

	client.send({
		protocol: "live-v1",
		id: "as-query",
		op: "query",
		target: { ...host.identity },
		command: { type: "interactive_input", text: "hello" },
	});
	const asQuery = await client.next();
	assert.equal(asQuery.ok, false);
	assert.match(asQuery.error.message, /not allowed for query/);

	client.send(dispatch({ type: "interactive_input", text: "x", literal: "yes" }, "bad-literal", host));
	const bad = await client.next();
	assert.equal(bad.ok, false);
	assert.equal(bad.error.code, "invalid_request");
	assert.deepEqual(host.dispatchCalls, [], "a malformed command never reaches the host");
});

test("expandPromptTemplates reaches the host verbatim on an ordinary prompt", async (t) => {
	const host = new InputHost();
	host.dispatchImpl = async () => ({ disposition: "started" });
	const { socketPath } = await fixture(t, host);
	const client = await JsonlClient.connect(socketPath);
	t.after(() => client.close());

	const command = { type: "prompt", id: "p-1", message: SLASH_LINE, expandPromptTemplates: false };
	client.send(dispatch(command, "prompt-literal", host));
	const reply = await client.next();
	assert.equal(reply.ok, true);
	assert.deepEqual(host.dispatchCalls[0], command, "false is forwarded, not dropped as falsy");

	const expanded = { type: "prompt", id: "p-2", message: TEMPLATE_LINE, expandPromptTemplates: true };
	client.send(dispatch(expanded, "prompt-expand", host));
	assert.equal((await client.next()).ok, true);
	assert.deepEqual(host.dispatchCalls[1], expanded);
});
