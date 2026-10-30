import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as http from "node:http";
import * as os from "node:os";
import * as path from "node:path";
import { createHash, randomUUID } from "node:crypto";
import { test, type TestContext } from "node:test";
import { initializeLiveTransferMarker, type LiveTransferMarker } from "../attachment-transfer.ts";
import {
	startLiveInputImageReader,
	type CurrentBranchInputImageCapableHost,
	type NativeAssociatedInputImageProjection,
} from "../input-image-reader.ts";
import type { CopyInputImageBytesOptions, LiveEvent, LiveIdentity, LiveSnapshot } from "../live-transport.ts";

const THREAD_ID = randomUUID();
const SESSION_ID = randomUUID();
const INPUT_ID = randomUUID();
const API_SOCKET = "/tmp/pi-rpc-socket-input-reader-test-api.sock";

function projection(bytes: Buffer): NativeAssociatedInputImageProjection {
	return {
		inputImageId: INPUT_ID,
		mediaType: "image/png",
		byteLength: bytes.length,
		sha256: createHash("sha256").update(bytes).digest("hex"),
		source: { type: "user_message_image", entryId: "entry-1", contentIndex: 0 },
		origin: "native",
		availability: { status: "available" },
	};
}

class InputHost implements CurrentBranchInputImageCapableHost {
	readonly target: LiveIdentity = { streamId: "input-stream", sessionId: SESSION_ID, sessionEpoch: 2 };
	bytes = Buffer.from("verified-live-input-image");
	leafId: string | null = "leaf-1";
	lookupCalls = 0;
	copyCalls = 0;
	lookupError: Error | undefined;
	copyImpl: CurrentBranchInputImageCapableHost["copyInputImageBytes"] | undefined;

	snapshot(): LiveSnapshot {
		return { ...this.target, sequence: 0 };
	}

	subscribe(_listener: (event: LiveEvent) => void): () => void {
		return () => undefined;
	}

	async dispatch(): Promise<unknown> {
		return undefined;
	}

	getCurrentBranchInputImage(): { leafId: string | null; inputImage: NativeAssociatedInputImageProjection } {
		this.lookupCalls++;
		if (this.lookupError) throw this.lookupError;
		return { leafId: this.leafId, inputImage: structuredClone(projection(this.bytes)) };
	}

	async copyInputImageBytes(
		target: LiveIdentity,
		inputImageId: string,
		options: CopyInputImageBytesOptions,
		write: (chunk: Uint8Array, offset: number) => void | Promise<void>,
	) {
		this.copyCalls++;
		if (this.copyImpl) return this.copyImpl(target, inputImageId, options, write);
		assert.deepEqual(target, this.target);
		assert.equal(inputImageId, INPUT_ID);
		for (let offset = 0; offset < this.bytes.length; offset += 5) {
			if (options.signal?.aborted) throw Object.assign(new Error("aborted"), { code: "aborted" });
			await write(this.bytes.subarray(offset, Math.min(this.bytes.length, offset + 5)), offset);
		}
		return { byteLength: this.bytes.length, sha256: createHash("sha256").update(this.bytes).digest("hex") };
	}

	close(): void {}
}

type Fixture = {
	root: string;
	inboxPath: string;
	socketPath: string;
	host: InputHost;
	reader: Awaited<ReturnType<typeof startLiveInputImageReader>>;
	marker: LiveTransferMarker;
};

async function fixture(t: TestContext): Promise<Fixture> {
	const root = await fs.promises.mkdtemp(path.join(os.tmpdir(), "pi-input-reader-test-"));
	const inboxPath = path.join(root, "inbox");
	await fs.promises.mkdir(inboxPath, { mode: 0o700 });
	await fs.promises.chmod(inboxPath, 0o700);
	const socketPath = path.join(root, `${SESSION_ID}.input.sock`);
	const host = new InputHost();
	const reader = await startLiveInputImageReader(host, { threadId: THREAD_ID, target: host.target, socketPath });
	const marker = await initializeLiveTransferMarker(
		{ threadId: THREAD_ID, inboxPath, apiSocketPath: API_SOCKET },
		host.target,
		["input-image-read-v1"],
		reader.descriptor,
	);
	reader.setTransferMarker(marker);
	t.after(async () => {
		await reader.close().catch(() => undefined);
		await marker.close().catch(() => undefined);
		await fs.promises.rm(root, { recursive: true, force: true });
	});
	return { root, inboxPath, socketPath, host, reader, marker };
}

async function request(
	socketPath: string,
	body: Record<string, unknown>,
): Promise<{ status: number; headers: http.IncomingHttpHeaders; body: Buffer }> {
	const payload = Buffer.from(JSON.stringify(body));
	return await new Promise((resolve, reject) => {
		const outgoing = http.request({
			socketPath,
			path: "/v1/input-images/read",
			method: "POST",
			headers: { "Content-Type": "application/json", "Content-Length": payload.length },
			agent: false,
		}, (response) => {
			const chunks: Buffer[] = [];
			response.on("data", (chunk: Buffer) => chunks.push(Buffer.from(chunk)));
			response.on("end", () => resolve({ status: response.statusCode ?? 0, headers: response.headers, body: Buffer.concat(chunks) }));
		});
		outgoing.once("error", reject);
		outgoing.end(payload);
	});
}

function requestBody(f: Fixture, generation = f.reader.descriptor.listener_generation) {
	return {
		protocol: "pi.input-image-read",
		version: 1,
		thread_id: THREAD_ID,
		target: { stream_id: f.host.target.streamId, session_id: SESSION_ID, session_epoch: f.host.target.sessionEpoch },
		listener_generation: generation,
		input_image_id: INPUT_ID,
	};
}

function errorCode(response: { body: Buffer }): string {
	return JSON.parse(response.body.toString("utf8")).error.code;
}

test("live input reader publishes the final descriptor and verifies complete bytes before success", async (t) => {
	const f = await fixture(t);
	const marker = JSON.parse(await fs.promises.readFile(path.join(f.inboxPath, "consumer.json"), "utf8"));
	assert.deepEqual(marker, {
		protocol: "sesh.pi-transfer",
		version: 1,
		thread_id: THREAD_ID,
		target: { stream_id: f.host.target.streamId, session_id: SESSION_ID, session_epoch: f.host.target.sessionEpoch },
		capabilities: ["input-image-read-v1"],
		input_image_reader: {
			protocol: "pi.input-image-read",
			version: 1,
			listener_generation: f.reader.descriptor.listener_generation,
			socket_suffix: ".input.sock",
		},
	});
	assert.equal((await fs.promises.stat(f.root)).mode & 0o777, 0o700);
	assert.equal((await fs.promises.stat(f.socketPath)).mode & 0o777, 0o600);

	const response = await request(f.socketPath, requestBody(f));
	assert.equal(response.status, 200);
	assert.deepEqual(response.body, f.host.bytes);
	assert.equal(response.headers["content-type"], "image/png");
	assert.equal(response.headers["cache-control"], "no-store");
	assert.equal(response.headers["x-pi-input-image-listener-generation"], f.reader.descriptor.listener_generation);
	assert.equal(response.headers["x-pi-input-image-sha256"], createHash("sha256").update(f.host.bytes).digest("hex"));
	assert.equal(response.headers["x-pi-input-image-digest-status"], "canonical-match");
	assert.equal(f.host.lookupCalls, 2);
	assert.equal(f.host.copyCalls, 1);
});

test("listener generation, pending authority and final branch are distinct failures", async (t) => {
	const f = await fixture(t);
	const wrongGeneration = await request(f.socketPath, requestBody(f, randomUUID()));
	assert.equal(wrongGeneration.status, 409);
	assert.equal(errorCode(wrongGeneration), "stale_listener_generation");
	assert.equal(f.host.lookupCalls, 0);

	f.host.lookupError = Object.assign(new Error("pending"), { code: "input_image_pending" });
	const pending = await request(f.socketPath, requestBody(f));
	assert.equal(pending.status, 409);
	assert.equal(errorCode(pending), "input_image_pending");
	assert.equal(f.host.copyCalls, 0);

	f.host.lookupError = undefined;
	f.host.copyImpl = async (_target, _inputImageId, _options, write) => {
		await write(f.host.bytes, 0);
		f.host.leafId = "leaf-2";
		return { byteLength: f.host.bytes.length, sha256: createHash("sha256").update(f.host.bytes).digest("hex") };
	};
	const stale = await request(f.socketPath, requestBody(f));
	assert.equal(stale.status, 409);
	assert.equal(errorCode(stale), "stale_branch");
	assert.notDeepEqual(stale.body, f.host.bytes);
});

test("count, digest and chunk verification fail before any image success body", async (t) => {
	const f = await fixture(t);
	f.host.copyImpl = async (_target, _inputImageId, _options, write) => {
		await write(f.host.bytes, 0);
		return { byteLength: f.host.bytes.length, sha256: "f".repeat(64) };
	};
	const digest = await request(f.socketPath, requestBody(f));
	assert.equal(digest.status, 502);
	assert.equal(errorCode(digest), "digest_mismatch");
	assert.notDeepEqual(digest.body, f.host.bytes);

	f.host.copyImpl = async (_target, _inputImageId, _options, write) => {
		await write(f.host.bytes, 0);
		return { byteLength: f.host.bytes.length - 1, sha256: createHash("sha256").update(f.host.bytes).digest("hex") };
	};
	const count = await request(f.socketPath, requestBody(f));
	assert.equal(count.status, 502);
	assert.equal(errorCode(count), "byte_count_mismatch");

	f.host.bytes = Buffer.alloc(256 * 1024 + 1, 7);
	f.host.copyImpl = async (_target, _inputImageId, _options, write) => {
		await write(f.host.bytes, 0);
		return { byteLength: f.host.bytes.length, sha256: createHash("sha256").update(f.host.bytes).digest("hex") };
	};
	const chunk = await request(f.socketPath, requestBody(f));
	assert.equal(chunk.status, 502);
	assert.equal(errorCode(chunk), "byte_count_mismatch");
});

test("reader allows two active copies and refuses an unbounded third request", async (t) => {
	const f = await fixture(t);
	let entered = 0;
	let release!: () => void;
	const gate = new Promise<void>((resolve) => { release = resolve; });
	f.host.copyImpl = async (_target, _inputImageId, options, write) => {
		entered++;
		await Promise.race([
			gate,
			new Promise<never>((_, reject) => options.signal?.addEventListener("abort", () => reject(Object.assign(new Error("aborted"), { code: "aborted" })), { once: true })),
		]);
		await write(f.host.bytes, 0);
		return { byteLength: f.host.bytes.length, sha256: createHash("sha256").update(f.host.bytes).digest("hex") };
	};
	const first = request(f.socketPath, requestBody(f));
	const second = request(f.socketPath, requestBody(f));
	while (entered < 2) await new Promise((resolve) => setTimeout(resolve, 2));
	const third = await request(f.socketPath, requestBody(f));
	assert.equal(third.status, 503);
	assert.equal(errorCode(third), "native_busy");
	release();
	assert.equal((await first).status, 200);
	assert.equal((await second).status, 200);
});

test("caller disconnect cancels the native copy", async (t) => {
	const f = await fixture(t);
	let entered!: () => void;
	const started = new Promise<void>((resolve) => { entered = resolve; });
	let aborted = false;
	f.host.copyImpl = async (_target, _inputImageId, options) => {
		entered();
		return await new Promise((_, reject) => {
			options.signal?.addEventListener("abort", () => {
				aborted = true;
				reject(Object.assign(new Error("aborted"), { code: "aborted" }));
			}, { once: true });
		});
	};
	const payload = Buffer.from(JSON.stringify(requestBody(f)));
	const outgoing = http.request({
		socketPath: f.socketPath,
		path: "/v1/input-images/read",
		method: "POST",
		headers: { "Content-Type": "application/json", "Content-Length": payload.length },
		agent: false,
	});
	outgoing.on("error", () => undefined);
	outgoing.end(payload);
	await started;
	outgoing.destroy();
	const deadline = Date.now() + 1000;
	while (!aborted && Date.now() < deadline) await new Promise((resolve) => setTimeout(resolve, 2));
	assert.equal(aborted, true);
});

test("old generation becomes unusable and late cleanup cannot remove a same-path replacement", async (t) => {
	const old = await fixture(t);
	const foreignDescriptor = {
		protocol: "pi.input-image-read" as const,
		version: 1 as const,
		listener_generation: randomUUID(),
		socket_suffix: ".input.sock" as const,
	};
	const foreignMarker = await initializeLiveTransferMarker(
		{ threadId: THREAD_ID, inboxPath: old.inboxPath, apiSocketPath: API_SOCKET },
		old.host.target,
		["input-image-read-v1"],
		foreignDescriptor,
	);
	t.after(() => foreignMarker.close().catch(() => undefined));
	const staleOld = await request(old.socketPath, requestBody(old));
	assert.equal(staleOld.status, 409);
	assert.equal(errorCode(staleOld), "stale_listener_generation");
	await old.reader.close();

	const replacementHost = new InputHost();
	const replacement = await startLiveInputImageReader(replacementHost, {
		threadId: THREAD_ID,
		target: replacementHost.target,
		socketPath: old.socketPath,
	});
	const replacementMarker = await initializeLiveTransferMarker(
		{ threadId: THREAD_ID, inboxPath: old.inboxPath, apiSocketPath: API_SOCKET },
		replacementHost.target,
		["input-image-read-v1"],
		replacement.descriptor,
	);
	replacement.setTransferMarker(replacementMarker);
	t.after(async () => {
		await replacement.close().catch(() => undefined);
		await replacementMarker.close().catch(() => undefined);
	});
	assert.notEqual(replacement.descriptor.listener_generation, old.reader.descriptor.listener_generation);

	await old.marker.close();
	await foreignMarker.close();
	assert.equal((await fs.promises.lstat(old.socketPath)).isSocket(), true);
	const currentMarker = JSON.parse(await fs.promises.readFile(path.join(old.inboxPath, "consumer.json"), "utf8"));
	assert.equal(currentMarker.input_image_reader.listener_generation, replacement.descriptor.listener_generation);

	const replacementFixture = { ...old, host: replacementHost, reader: replacement, marker: replacementMarker };
	const response = await request(old.socketPath, requestBody(replacementFixture));
	assert.equal(response.status, 200);
	assert.deepEqual(response.body, replacementHost.bytes);
});
