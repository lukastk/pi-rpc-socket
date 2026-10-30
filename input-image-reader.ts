import * as fs from "node:fs";
import * as http from "node:http";
import * as net from "node:net";
import * as path from "node:path";
import { createHash, randomUUID } from "node:crypto";
import {
	isCanonicalUuid,
	type LiveInputImageReaderDescriptor,
	type LiveTransferMarker,
} from "./attachment-transfer.ts";
import { inputImageSocketPath } from "./socket-paths.ts";
import type {
	CopyInputImageBytesOptions,
	CopyInputImageBytesResult,
	InputImageChunkWriter,
	LiveIdentity,
	LiveTransportHost,
} from "./live-transport.ts";

export const LIVE_INPUT_IMAGE_READER_CAPABILITY = {
	protocol: "pi.input-image-read",
	version: 1,
	transport: "http+unix",
	socketSuffix: ".input.sock",
	metadataLookup: "current-branch-id",
	maxImageBytes: 32 * 1024 * 1024,
	copyChunkBytes: 256 * 1024,
	maxConcurrentReads: 2,
	completeBeforeSuccess: true,
	pendingReads: false,
	binaryInLive: false,
} as const;

const REQUEST_PATH = "/v1/input-images/read";
const MAX_REQUEST_BYTES = 8 * 1024;
const OPERATION_TIMEOUT_MS = 12_000;
const MAX_ERROR_BYTES = 16 * 1024;
const INPUT_IMAGE_ID_PATTERN = /^(?:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|[0-9a-f]{64})$/;
const DIGEST_PATTERN = /^[0-9a-f]{64}$/;
const MEDIA_TYPE_PATTERN = /^[A-Za-z0-9!#$&^_.+-]+\/[A-Za-z0-9!#$&^_.+-]+$/;

export interface NativeAssociatedInputImageProjection {
	inputImageId: string;
	mediaType: string;
	byteLength: number;
	sha256?: string;
	source:
		| { type: "user_message_image"; entryId: string; contentIndex: number }
		| { type: "user_context_edit_image"; entryId: string; targetEntryId: string; contentIndex: number };
	origin: "native" | "legacy_user_image";
	availability:
		| { status: "available" }
		| { status: "unavailable"; reason: "invalid_bytes" | "byte_limit" | "source_missing" | "not_persisted"; message: string };
}

export interface NativeCurrentBranchInputImage {
	leafId: string | null;
	inputImage: NativeAssociatedInputImageProjection;
}

export interface CurrentBranchInputImageCapableHost extends LiveTransportHost {
	getCurrentBranchInputImage(target: LiveIdentity, inputImageId: string): NativeCurrentBranchInputImage;
	copyInputImageBytes(
		target: LiveIdentity,
		inputImageId: string,
		options: CopyInputImageBytesOptions,
		write: InputImageChunkWriter,
	): Promise<CopyInputImageBytesResult>;
}

export interface StartLiveInputImageReaderOptions {
	threadId: string;
	socketPath: string;
	target: LiveIdentity;
	onError?: (error: unknown) => void;
}

export interface StartLiveSessionInputImageReaderOptions extends Omit<StartLiveInputImageReaderOptions, "socketPath" | "target"> {
	socketsDir?: string;
}

export interface LiveInputImageReader {
	readonly capability: typeof LIVE_INPUT_IMAGE_READER_CAPABILITY;
	readonly descriptor: Readonly<LiveInputImageReaderDescriptor>;
	readonly socketPath: string;
	setTransferMarker(marker: LiveTransferMarker | undefined): void;
	close(): Promise<void>;
}

type ReaderRequest = {
	protocol: "pi.input-image-read";
	version: 1;
	thread_id: string;
	target: { stream_id: string; session_id: string; session_epoch: number };
	listener_generation: string;
	input_image_id: string;
};

type NormalizedProjection = NativeAssociatedInputImageProjection;

type ReaderErrorCode =
	| "invalid_request"
	| "stale_target"
	| "stale_branch"
	| "stale_listener_generation"
	| "input_image_pending"
	| "input_image_not_found"
	| "input_image_unavailable"
	| "byte_limit"
	| "invalid_native_metadata"
	| "byte_count_mismatch"
	| "digest_mismatch"
	| "native_busy"
	| "runtime_unavailable"
	| "native_timeout"
	| "unsupported_native_capability";

class LiveInputImageReaderError extends Error {
	constructor(readonly code: ReaderErrorCode, readonly statusCode: number, message: string) {
		super(message);
	}
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return value !== null && typeof value === "object" && !Array.isArray(value);
}

function exactKeys(value: Record<string, unknown>, required: readonly string[], optional: readonly string[] = []): boolean {
	const allowed = new Set([...required, ...optional]);
	return required.every((key) => Object.hasOwn(value, key)) && Object.keys(value).every((key) => allowed.has(key));
}

function safeIdentityText(value: unknown, maxBytes: number): value is string {
	return typeof value === "string" && Buffer.byteLength(value) >= 1 && Buffer.byteLength(value) <= maxBytes
		&& !/[\u0000-\u001f\u007f]/.test(value);
}

function safeInteger(value: unknown): value is number {
	return Number.isSafeInteger(value) && (value as number) >= 0;
}

function sameIdentity(left: LiveIdentity, right: LiveIdentity): boolean {
	return left.streamId === right.streamId && left.sessionEpoch === right.sessionEpoch && left.sessionId === right.sessionId;
}

function wireTarget(target: LiveIdentity): ReaderRequest["target"] {
	return { stream_id: target.streamId, session_id: target.sessionId, session_epoch: target.sessionEpoch };
}

function parseRequest(value: unknown): ReaderRequest {
	if (!isRecord(value) || !exactKeys(value, [
		"protocol", "version", "thread_id", "target", "listener_generation", "input_image_id",
	])) throw new LiveInputImageReaderError("invalid_request", 400, "Private input-image request has an invalid shape");
	if (!isRecord(value.target) || !exactKeys(value.target, ["stream_id", "session_id", "session_epoch"])) {
		throw new LiveInputImageReaderError("invalid_request", 400, "Private input-image target has an invalid shape");
	}
	if (value.protocol !== "pi.input-image-read" || value.version !== 1 || !isCanonicalUuid(value.thread_id)
		|| !safeIdentityText(value.target.stream_id, 128) || !isCanonicalUuid(value.target.session_id)
		|| !safeInteger(value.target.session_epoch) || !isCanonicalUuid(value.listener_generation)
		|| typeof value.input_image_id !== "string" || !INPUT_IMAGE_ID_PATTERN.test(value.input_image_id)) {
		throw new LiveInputImageReaderError("invalid_request", 400, "Private input-image request identity is invalid");
	}
	return value as unknown as ReaderRequest;
}

function parseAvailability(value: unknown): NativeAssociatedInputImageProjection["availability"] {
	if (!isRecord(value) || typeof value.status !== "string") {
		throw new LiveInputImageReaderError("invalid_native_metadata", 422, "Native input-image availability is invalid");
	}
	if (value.status === "available" && exactKeys(value, ["status"])) return { status: "available" };
	if (value.status === "unavailable" && exactKeys(value, ["status", "reason", "message"])
		&& ["invalid_bytes", "byte_limit", "source_missing", "not_persisted"].includes(value.reason as string)
		&& safeIdentityText(value.message, 1024)) {
		return { status: "unavailable", reason: value.reason as "invalid_bytes" | "byte_limit" | "source_missing" | "not_persisted", message: value.message };
	}
	throw new LiveInputImageReaderError("invalid_native_metadata", 422, "Native input-image availability is invalid");
}

function parseSource(value: unknown): NativeAssociatedInputImageProjection["source"] {
	if (!isRecord(value) || !safeIdentityText(value.entryId, 512) || !safeInteger(value.contentIndex)) {
		throw new LiveInputImageReaderError("invalid_native_metadata", 422, "Native input-image source is invalid");
	}
	if (value.type === "user_message_image" && exactKeys(value, ["type", "entryId", "contentIndex"])) {
		return { type: "user_message_image", entryId: value.entryId, contentIndex: value.contentIndex };
	}
	if (value.type === "user_context_edit_image" && exactKeys(value, ["type", "entryId", "targetEntryId", "contentIndex"])
		&& safeIdentityText(value.targetEntryId, 512)) {
		return {
			type: "user_context_edit_image",
			entryId: value.entryId,
			targetEntryId: value.targetEntryId,
			contentIndex: value.contentIndex,
		};
	}
	throw new LiveInputImageReaderError("invalid_native_metadata", 422, "Native input-image source is invalid");
}

function parseProjection(value: unknown, expectedId: string): NormalizedProjection {
	if (!isRecord(value) || !exactKeys(value, [
		"inputImageId", "mediaType", "byteLength", "source", "origin", "availability",
	], ["sha256"])) throw new LiveInputImageReaderError("invalid_native_metadata", 422, "Native input-image metadata has an invalid shape");
	if (value.inputImageId !== expectedId || typeof value.mediaType !== "string" || Buffer.byteLength(value.mediaType) < 1
		|| Buffer.byteLength(value.mediaType) > 127 || !MEDIA_TYPE_PATTERN.test(value.mediaType)
		|| !safeInteger(value.byteLength) || (value.sha256 !== undefined && (typeof value.sha256 !== "string" || !DIGEST_PATTERN.test(value.sha256)))
		|| (value.origin !== "native" && value.origin !== "legacy_user_image")) {
		throw new LiveInputImageReaderError("invalid_native_metadata", 422, "Native input-image metadata is invalid");
	}
	return {
		inputImageId: value.inputImageId,
		mediaType: value.mediaType,
		byteLength: value.byteLength,
		...(value.sha256 === undefined ? {} : { sha256: value.sha256 }),
		source: parseSource(value.source),
		origin: value.origin,
		availability: parseAvailability(value.availability),
	};
}

function parseLookup(value: unknown, expectedId: string): { leafId: string | null; inputImage: NormalizedProjection } {
	if (!isRecord(value) || !exactKeys(value, ["leafId", "inputImage"])
		|| (value.leafId !== null && !safeIdentityText(value.leafId, 512))) {
		throw new LiveInputImageReaderError("invalid_native_metadata", 422, "Native current-branch input-image result is invalid");
	}
	return { leafId: value.leafId, inputImage: parseProjection(value.inputImage, expectedId) };
}

function sameProjection(left: NormalizedProjection, right: NormalizedProjection): boolean {
	return JSON.stringify(left) === JSON.stringify(right);
}

function mapNativeError(error: unknown): LiveInputImageReaderError {
	const code = isRecord(error) && typeof error.code === "string" ? error.code : undefined;
	switch (code) {
		case "input_image_pending": return new LiveInputImageReaderError("input_image_pending", 409, "Input image is not durably associated");
		case "input_image_not_found": return new LiveInputImageReaderError("input_image_not_found", 404, "Input image is not present on the current branch");
		case "input_image_unavailable":
		case "invalid_bytes": return new LiveInputImageReaderError("input_image_unavailable", 409, "Input-image bytes are unavailable");
		case "stale_branch": return new LiveInputImageReaderError("stale_branch", 409, "Input-image branch authority changed during the read");
		case "stale_session": return new LiveInputImageReaderError("stale_target", 409, "Input-image target is stale");
		case "byte_count_mismatch": return new LiveInputImageReaderError("byte_count_mismatch", 502, "Native input-image byte count did not match metadata");
		case "digest_mismatch": return new LiveInputImageReaderError("digest_mismatch", 502, "Native input-image digest did not match metadata");
		case "timeout": return new LiveInputImageReaderError("native_timeout", 504, "Native input-image read timed out");
		case "aborted": return new LiveInputImageReaderError("runtime_unavailable", 503, "Native input-image read was aborted");
		case "invalid_limits":
		case "consumer_failed": return new LiveInputImageReaderError("runtime_unavailable", 503, "Native input-image reader is unavailable");
	}
	const message = error instanceof Error ? error.message : "";
	if (/target is stale|session before copying|reconcile the session/i.test(message)) {
		return new LiveInputImageReaderError("stale_target", 409, "Input-image target is stale");
	}
	if (/closed|disposed|replacing|runtime is|session changed/i.test(message)) {
		return new LiveInputImageReaderError("runtime_unavailable", 503, "Native input-image runtime is unavailable");
	}
	return new LiveInputImageReaderError("runtime_unavailable", 503, "Native input-image reader failed");
}

async function prepareSocket(socketPath: string): Promise<void> {
	const directory = path.dirname(socketPath);
	await fs.promises.mkdir(directory, { recursive: true, mode: 0o700 });
	await fs.promises.chmod(directory, 0o700);
	let stat: fs.Stats;
	try {
		stat = await fs.promises.lstat(socketPath);
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code === "ENOENT") return;
		throw error;
	}
	if (!stat.isSocket()) throw new Error(`Refusing to replace non-socket input-image path: ${socketPath}`);
	const probe = await new Promise<"active" | "stale">((resolve, reject) => {
		const socket = net.createConnection(socketPath);
		let settled = false;
		const finish = (value: "active" | "stale") => {
			if (settled) return;
			settled = true;
			socket.destroy();
			resolve(value);
		};
		socket.once("connect", () => finish("active"));
		socket.once("error", (error: NodeJS.ErrnoException) => {
			if (error.code === "ECONNREFUSED" || error.code === "ENOENT") finish("stale");
			else if (!settled) {
				settled = true;
				socket.destroy();
				reject(new Error(`Cannot safely classify existing input-image socket: ${error.code ?? error.message}`, { cause: error }));
			}
		});
		socket.setTimeout(500, () => {
			if (settled) return;
			settled = true;
			socket.destroy();
			reject(new Error("Timed out probing existing input-image socket; refusing to unlink it"));
		});
	});
	if (probe === "active") throw new Error(`Input-image reader socket is already active: ${socketPath}`);
	await fs.promises.unlink(socketPath).catch((error: NodeJS.ErrnoException) => {
		if (error.code !== "ENOENT") throw error;
	});
}

async function readJsonBody(request: http.IncomingMessage, signal: AbortSignal): Promise<unknown> {
	if (request.headers["transfer-encoding"] !== undefined) {
		throw new LiveInputImageReaderError("invalid_request", 400, "Transfer encoding is not supported");
	}
	if (request.headers["content-type"] !== "application/json") {
		throw new LiveInputImageReaderError("invalid_request", 400, "Content-Type must be application/json");
	}
	const contentLength = Number(request.headers["content-length"]);
	if (!Number.isSafeInteger(contentLength) || contentLength < 1 || contentLength > MAX_REQUEST_BYTES) {
		throw new LiveInputImageReaderError("invalid_request", 400, "Content-Length is outside the private request bound");
	}
	const chunks: Buffer[] = [];
	let total = 0;
	for await (const chunk of request) {
		if (signal.aborted) throw new LiveInputImageReaderError("runtime_unavailable", 503, "Private input-image request was cancelled");
		const bytes = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
		total += bytes.length;
		if (total > MAX_REQUEST_BYTES || total > contentLength) {
			throw new LiveInputImageReaderError("invalid_request", 400, "Private request body exceeds its declared bound");
		}
		chunks.push(bytes);
	}
	if (signal.aborted) throw new LiveInputImageReaderError("runtime_unavailable", 503, "Private input-image request was cancelled");
	if (total !== contentLength) throw new LiveInputImageReaderError("invalid_request", 400, "Private request body length is incomplete");
	try {
		return JSON.parse(Buffer.concat(chunks, total).toString("utf8"));
	} catch {
		throw new LiveInputImageReaderError("invalid_request", 400, "Private request body is not strict JSON");
	}
}

function writeError(response: http.ServerResponse, error: LiveInputImageReaderError): void {
	if (response.destroyed || response.headersSent) {
		response.destroy();
		return;
	}
	let body = Buffer.from(JSON.stringify({
		protocol: "pi.input-image-read",
		version: 1,
		status: "error",
		error: { code: error.code, message: error.message },
	}));
	if (body.length > MAX_ERROR_BYTES) body = Buffer.from('{"protocol":"pi.input-image-read","version":1,"status":"error","error":{"code":"runtime_unavailable","message":"Input-image reader failed"}}');
	response.writeHead(error.statusCode, {
		"Content-Type": "application/json",
		"Content-Length": body.length,
		"Cache-Control": "no-store",
		"X-Content-Type-Options": "nosniff",
	});
	response.end(body);
}

export async function startLiveInputImageReader(
	host: CurrentBranchInputImageCapableHost,
	options: StartLiveInputImageReaderOptions,
): Promise<LiveInputImageReader> {
	if (!isCanonicalUuid(options.threadId) || !isCanonicalUuid(options.target.sessionId)) {
		throw new Error("Live input-image reader identity is invalid");
	}
	const descriptor: LiveInputImageReaderDescriptor = {
		protocol: "pi.input-image-read",
		version: 1,
		listener_generation: randomUUID(),
		socket_suffix: ".input.sock",
	};
	const onError = options.onError ?? ((error: unknown) => console.error("Live input-image reader error:", error));
	let transferMarker: LiveTransferMarker | undefined;
	let closed = false;
	let closing: Promise<void> | undefined;
	let active = 0;
	let ownedSocket: { dev: number; ino: number } | undefined;
	const lifecycle = new AbortController();
	const connections = new Set<net.Socket>();
	const handlers = new Set<Promise<void>>();

	const requireMarker = async (): Promise<void> => {
		if (!transferMarker || !(await transferMarker.matchesInputImageReader(descriptor))) {
			throw new LiveInputImageReaderError("stale_listener_generation", 409, "Input-image listener generation is stale");
		}
	};

	const handle = async (request: http.IncomingMessage, response: http.ServerResponse): Promise<void> => {
		if (request.method !== "POST" || request.url !== REQUEST_PATH) {
			writeError(response, new LiveInputImageReaderError("invalid_request", 400, "Unsupported private input-image operation"));
			return;
		}
		if (closed) {
			writeError(response, new LiveInputImageReaderError("runtime_unavailable", 503, "Input-image reader is closing"));
			return;
		}
		if (active >= LIVE_INPUT_IMAGE_READER_CAPABILITY.maxConcurrentReads) {
			writeError(response, new LiveInputImageReaderError("native_busy", 503, "Input-image reader capacity is in use"));
			return;
		}
		active++;
		const operation = new AbortController();
		const timeout = setTimeout(() => operation.abort(new Error("deadline")), OPERATION_TIMEOUT_MS);
		timeout.unref?.();
		const abort = () => operation.abort(new Error("request cancelled"));
		const lifecycleAbort = () => operation.abort(lifecycle.signal.reason);
		const operationAbort = () => {
			if (!request.complete) request.destroy();
		};
		request.once("aborted", abort);
		response.once("close", () => {
			if (!response.writableEnded) abort();
		});
		lifecycle.signal.addEventListener("abort", lifecycleAbort, { once: true });
		operation.signal.addEventListener("abort", operationAbort, { once: true });
		try {
			const parsed = parseRequest(await readJsonBody(request, operation.signal));
			const requestTarget: LiveIdentity = {
				streamId: parsed.target.stream_id,
				sessionId: parsed.target.session_id,
				sessionEpoch: parsed.target.session_epoch,
			};
			if (parsed.thread_id !== options.threadId || !sameIdentity(requestTarget, options.target)) {
				throw new LiveInputImageReaderError("stale_target", 409, "Input-image target is stale");
			}
			if (parsed.listener_generation !== descriptor.listener_generation) {
				throw new LiveInputImageReaderError("stale_listener_generation", 409, "Input-image listener generation is stale");
			}
			await requireMarker();
			if (operation.signal.aborted) throw new LiveInputImageReaderError("runtime_unavailable", 503, "Input-image read was cancelled");

			let initial: { leafId: string | null; inputImage: NormalizedProjection };
			try {
				initial = parseLookup(host.getCurrentBranchInputImage(requestTarget, parsed.input_image_id), parsed.input_image_id);
			} catch (error) {
				if (error instanceof LiveInputImageReaderError) throw error;
				throw mapNativeError(error);
			}
			if (initial.inputImage.availability.status !== "available") {
				throw new LiveInputImageReaderError("input_image_unavailable", 409, "Input-image bytes are unavailable");
			}
			if (initial.inputImage.byteLength > LIVE_INPUT_IMAGE_READER_CAPABILITY.maxImageBytes) {
				throw new LiveInputImageReaderError("byte_limit", 413, "Input image exceeds the live byte limit");
			}

			const chunks: Buffer[] = [];
			const digest = createHash("sha256");
			let total = 0;
			let result: CopyInputImageBytesResult;
			try {
				result = await host.copyInputImageBytes(
					requestTarget,
					parsed.input_image_id,
					{
						maxBytes: LIVE_INPUT_IMAGE_READER_CAPABILITY.maxImageBytes,
						chunkBytes: LIVE_INPUT_IMAGE_READER_CAPABILITY.copyChunkBytes,
						timeoutMs: OPERATION_TIMEOUT_MS,
						signal: operation.signal,
					},
					(chunk, offset) => {
						if (operation.signal.aborted) throw new Error("input-image copy aborted");
						if (offset !== total || chunk.byteLength < 1 || chunk.byteLength > LIVE_INPUT_IMAGE_READER_CAPABILITY.copyChunkBytes) {
							throw new LiveInputImageReaderError("byte_count_mismatch", 502, "Native input-image chunks are not contiguous and bounded");
						}
						if (total + chunk.byteLength > initial.inputImage.byteLength
							|| total + chunk.byteLength > LIVE_INPUT_IMAGE_READER_CAPABILITY.maxImageBytes) {
							throw new LiveInputImageReaderError("byte_limit", 413, "Native input-image bytes exceed their declared bound");
						}
						const copy = Buffer.from(chunk);
						chunks.push(copy);
						digest.update(copy);
						total += copy.length;
					},
				);
			} catch (error) {
				if (error instanceof LiveInputImageReaderError) throw error;
				if (operation.signal.aborted) {
					if ((operation.signal.reason as Error | undefined)?.message === "deadline") {
						throw new LiveInputImageReaderError("native_timeout", 504, "Native input-image read timed out");
					}
					throw new LiveInputImageReaderError("runtime_unavailable", 503, "Native input-image read was cancelled");
				}
				throw mapNativeError(error);
			}
			const sha256 = digest.digest("hex");
			if (total !== initial.inputImage.byteLength || result.byteLength !== total) {
				throw new LiveInputImageReaderError("byte_count_mismatch", 502, "Native input-image byte count did not match metadata");
			}
			if (!DIGEST_PATTERN.test(result.sha256) || result.sha256 !== sha256
				|| (initial.inputImage.sha256 !== undefined && initial.inputImage.sha256 !== sha256)) {
				throw new LiveInputImageReaderError("digest_mismatch", 502, "Native input-image digest did not match metadata");
			}

			let final: { leafId: string | null; inputImage: NormalizedProjection };
			try {
				final = parseLookup(host.getCurrentBranchInputImage(requestTarget, parsed.input_image_id), parsed.input_image_id);
			} catch (error) {
				if (error instanceof LiveInputImageReaderError) throw error;
				throw mapNativeError(error);
			}
			if (final.leafId !== initial.leafId || !sameProjection(final.inputImage, initial.inputImage)) {
				throw new LiveInputImageReaderError("stale_branch", 409, "Input-image branch authority changed during the read");
			}
			await requireMarker();
			if (operation.signal.aborted || closed) {
				throw new LiveInputImageReaderError("runtime_unavailable", 503, "Input-image read was cancelled");
			}
			const body = Buffer.concat(chunks, total);
			response.writeHead(200, {
				"Content-Type": initial.inputImage.mediaType,
				"Content-Length": body.length,
				"Cache-Control": "no-store",
				"X-Content-Type-Options": "nosniff",
				"X-Pi-Input-Image-Protocol": "pi.input-image-read",
				"X-Pi-Input-Image-Version": "1",
				"X-Pi-Input-Image-Listener-Generation": descriptor.listener_generation,
				"X-Pi-Input-Image-ID": parsed.input_image_id,
				"X-Pi-Input-Image-Thread-ID": options.threadId,
				"X-Pi-Input-Image-Stream-ID": options.target.streamId,
				"X-Pi-Input-Image-Session-ID": options.target.sessionId,
				"X-Pi-Input-Image-Session-Epoch": String(options.target.sessionEpoch),
				"X-Pi-Input-Image-SHA256": sha256,
				"X-Pi-Input-Image-Digest-Status": initial.inputImage.sha256 ? "canonical-match" : "computed-only",
				...(initial.inputImage.sha256 ? { "X-Pi-Input-Image-Canonical-SHA256": initial.inputImage.sha256 } : {}),
			});
			response.end(body);
		} catch (error) {
			if (request.aborted || response.destroyed) return;
			writeError(response, error instanceof LiveInputImageReaderError ? error : mapNativeError(error));
		} finally {
			clearTimeout(timeout);
			request.off("aborted", abort);
			lifecycle.signal.removeEventListener("abort", lifecycleAbort);
			operation.signal.removeEventListener("abort", operationAbort);
			active--;
		}
	};

	await prepareSocket(options.socketPath);
	const server = http.createServer((request, response) => {
		const handler = handle(request, response);
		handlers.add(handler);
		handler.catch(onError).finally(() => handlers.delete(handler));
	});
	server.headersTimeout = 5_000;
	server.requestTimeout = OPERATION_TIMEOUT_MS;
	server.keepAliveTimeout = 1_000;
	server.maxRequestsPerSocket = 1;
	server.on("connection", (socket) => {
		connections.add(socket);
		socket.once("close", () => connections.delete(socket));
	});
	try {
		await new Promise<void>((resolve, reject) => {
			const onListening = () => {
				server.off("error", onStartupError);
				resolve();
			};
			const onStartupError = (error: Error) => {
				server.off("listening", onListening);
				reject(error);
			};
			server.once("listening", onListening);
			server.once("error", onStartupError);
			server.listen(options.socketPath);
		});
		const stat = await fs.promises.lstat(options.socketPath);
		if (!stat.isSocket()) throw new Error("Bound input-image path is not a Unix socket");
		ownedSocket = { dev: stat.dev, ino: stat.ino };
		await fs.promises.chmod(options.socketPath, 0o600);
	} catch (error) {
		for (const socket of connections) socket.destroy();
		await new Promise<void>((resolve) => server.close(() => resolve()));
		if (ownedSocket) {
			try {
				const current = await fs.promises.lstat(options.socketPath);
				if (current.isSocket() && current.dev === ownedSocket.dev && current.ino === ownedSocket.ino) {
					await fs.promises.unlink(options.socketPath);
				}
			} catch (cleanupError) {
				if ((cleanupError as NodeJS.ErrnoException).code !== "ENOENT") onError(cleanupError);
			}
		}
		throw error;
	}
	server.on("error", onError);

	return {
		capability: LIVE_INPUT_IMAGE_READER_CAPABILITY,
		descriptor,
		socketPath: options.socketPath,
		setTransferMarker(marker) {
			const candidate = marker?.inputImageReader;
			if (!candidate || candidate.protocol !== descriptor.protocol || candidate.version !== descriptor.version
				|| candidate.listener_generation !== descriptor.listener_generation || candidate.socket_suffix !== descriptor.socket_suffix) {
				transferMarker = undefined;
				return;
			}
			transferMarker = marker;
		},
		async close() {
			if (closing) return closing;
			closing = (async () => {
				closed = true;
				lifecycle.abort(new Error("input-image reader closed"));
				const serverClosed = new Promise<void>((resolve) => server.close(() => resolve()));
				for (const socket of connections) socket.destroy();
				await serverClosed;
				const pendingHandlers = [...handlers];
				if (pendingHandlers.length > 0) {
					let settleTimer: NodeJS.Timeout | undefined;
					try {
						await Promise.race([
							Promise.allSettled(pendingHandlers),
							new Promise<void>((resolve) => {
								settleTimer = setTimeout(resolve, 1000);
								settleTimer.unref?.();
							}),
						]);
					} finally {
						if (settleTimer) clearTimeout(settleTimer);
					}
				}
				const ownsMarker = transferMarker ? await transferMarker.matchesInputImageReader(descriptor).catch(() => false) : true;
				if (ownedSocket && ownsMarker) {
					try {
						const current = await fs.promises.lstat(options.socketPath);
						if (current.isSocket() && current.dev === ownedSocket.dev && current.ino === ownedSocket.ino) {
							await fs.promises.unlink(options.socketPath);
						}
					} catch (error) {
						if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
					}
				}
			})();
			return closing;
		},
	};
}

export function startLiveSessionInputImageReader(
	host: CurrentBranchInputImageCapableHost,
	target: LiveIdentity,
	options: StartLiveSessionInputImageReaderOptions,
): Promise<LiveInputImageReader> {
	const { socketsDir, ...readerOptions } = options;
	return startLiveInputImageReader(host, {
		...readerOptions,
		target,
		socketPath: inputImageSocketPath(target.sessionId, socketsDir),
	});
}
