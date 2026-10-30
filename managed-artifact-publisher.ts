import * as http from "node:http";
import * as path from "node:path";
import { createHash } from "node:crypto";
import type { LiveEvent, LiveIdentity, LiveSnapshot, LiveTransportHost } from "./live-transport.ts";
import { isCanonicalUuid } from "./attachment-transfer.ts";

export const MANAGED_ARTIFACT_CAPABILITY = {
	protocol: "sesh.pi-artifact-map",
	version: 1,
	delivery: "browser-query",
	maxArtifactBytes: 32 * 1024 * 1024,
	copyChunkBytes: 256 * 1024,
	binaryInLive: false,
} as const;

export const MANAGED_ARTIFACT_LIMITS = {
	maxConcurrentCopies: 2,
	maxQueuedPublications: 64,
	maxObservedMappings: 256,
	maxObservedMetadataBytes: 2 * 1024 * 1024,
	maxBufferedNativeEvents: 256,
	maxBufferedNativeEventBytes: 512 * 1024,
	maxArtifactBytes: 32 * 1024 * 1024,
	copyChunkBytes: 256 * 1024,
	copyTimeoutMs: 10_000,
	seshRequestTimeoutMs: 15_000,
	maxResponseBytes: 64 * 1024,
	maxReconciliationPages: 4,
} as const;

export interface NativeManagedArtifactKey {
	threadId: string;
	nativeSessionId: string;
	publicationId: string;
}

export interface NativeArtifactAvailability {
	status: "available" | "unavailable";
	reason?: "source_not_retained" | "source_failed" | "invalid_bytes" | "byte_limit" | "lifecycle_ended";
	message?: string;
}

export interface NativeArtifactProjection {
	publicationId: string;
	kind: "image" | "file";
	displayName: string;
	mediaType: string;
	byteLength: number;
	sha256?: string;
	disposition: "inline" | "attachment";
	retention: "session_inline" | "runtime_source";
	origin: "native" | "legacy_inline_image";
	availability: NativeArtifactAvailability;
}

export interface ArtifactCapableLiveTransportHost extends LiveTransportHost {
	copyArtifactBytes(
		target: LiveIdentity,
		publicationId: string,
		options: { maxBytes: number; chunkBytes: number; timeoutMs: number; signal?: AbortSignal },
		write: (chunk: Uint8Array, offset: number) => void | Promise<void>,
	): Promise<{ byteLength: number; sha256: string }>;
}

export interface ManagedArtifactPublisherLimits {
	maxConcurrentCopies: number;
	maxQueuedPublications: number;
	maxObservedMappings: number;
	maxObservedMetadataBytes: number;
	maxBufferedNativeEvents: number;
	maxBufferedNativeEventBytes: number;
	maxArtifactBytes: number;
	copyChunkBytes: number;
	copyTimeoutMs: number;
	seshRequestTimeoutMs: number;
	maxResponseBytes: number;
	maxReconciliationPages: number;
}

interface WireTarget {
	stream_id: string;
	session_id: string;
	session_epoch: number;
}

interface WirePublication {
	publication_id: string;
	kind: "image" | "file";
	name: string;
	media_type: string;
	size: number;
	sha256: string | null;
	disposition: "inline" | "attachment";
	retention: "session_inline" | "runtime_source";
	origin: "native" | "legacy_inline_image";
	availability:
		| { status: "available" }
		| {
				status: "unavailable";
				reason: "source_not_retained" | "source_failed" | "invalid_bytes" | "byte_limit" | "lifecycle_ended";
				message: string;
		  };
}

interface ManagedArtifactReference {
	id: string;
	thread_id: string;
	target: WireTarget;
	name: string;
	media_type: string;
	size: number;
	sha256: string;
	disposition: "inline" | "attachment";
	expires_at_unix_ms: number;
}

export type ManagedArtifactRuntimeMapping =
	| {
			mapping_id: string;
			publication_id: string;
			status: "staging";
			producer_target: WireTarget;
			artifact_id: string;
			updated_at_unix_ms: number;
	  }
	| {
			mapping_id: string;
			publication_id: string;
			status: "ready";
			producer_target: WireTarget;
			artifact: ManagedArtifactReference;
			updated_at_unix_ms: number;
	  }
	| {
			mapping_id: string;
			publication_id: string;
			status: "unavailable";
			unavailable: { source: "native" | "native_copy" | "sesh"; code: string; message: string };
			updated_at_unix_ms: number;
	  }
	| {
			mapping_id: string;
			publication_id: string;
			status: "expired";
			artifact_id: string;
			expired_at_unix_ms: number;
			updated_at_unix_ms: number;
	  };

interface RuntimeEnvelope {
	schema: 55 | 56;
	protocol: "sesh.pi-artifact-map";
	version: 1;
	mapping: ManagedArtifactRuntimeMapping;
}

export interface ManagedArtifactResolveRequest {
	thread_id: string;
	consumer_target: WireTarget;
	native_session_id: string;
	publication: WirePublication;
}

export interface ManagedArtifactFailRequest {
	thread_id: string;
	consumer_target: WireTarget;
	native_session_id: string;
	publication_id: string;
	failure: {
		source: "native_copy" | "sesh";
		code:
			| "artifact_not_found"
			| "artifact_unavailable"
			| "source_failed"
			| "byte_count_mismatch"
			| "digest_mismatch"
			| "invalid_limits"
			| "quota_exceeded";
		message: string;
	};
}

export interface ManagedArtifactSeshClient {
	resolveMapping(request: ManagedArtifactResolveRequest, signal: AbortSignal): Promise<RuntimeEnvelope>;
	putMappingContent(
		input: {
			threadId: string;
			mappingId: string;
			consumerTarget: WireTarget;
			contentLength: number;
		},
		copy: (write: (chunk: Uint8Array, offset: number) => Promise<void>) => Promise<{ byteLength: number; sha256: string }>,
		signal: AbortSignal,
	): Promise<{ envelope: RuntimeEnvelope; copyResult: { byteLength: number; sha256: string } }>;
	failMapping(request: ManagedArtifactFailRequest, signal: AbortSignal): Promise<RuntimeEnvelope>;
}

export interface ManagedArtifactPublisherOptions {
	host: ArtifactCapableLiveTransportHost;
	threadId: string;
	apiSocketPath: string;
	expectedTarget?: LiveIdentity;
	client?: ManagedArtifactSeshClient;
	limits?: Partial<ManagedArtifactPublisherLimits>;
	onError?: (error: unknown) => void;
}

export interface ManagedArtifactPublisher {
	readonly capability: typeof MANAGED_ARTIFACT_CAPABILITY;
	close(): Promise<void>;
}

class ManagedArtifactProtocolError extends Error {
	constructor(readonly code: string, message: string, readonly statusCode = 0) {
		super(message);
		this.name = "ManagedArtifactProtocolError";
	}
}

const PUBLICATION_PATTERN = /^(?:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|[0-9a-f]{64})$/;
const DIGEST_PATTERN = /^[0-9a-f]{64}$/;
const SAFE_ERROR_CODE_PATTERN = /^[a-z][a-z0-9_]{0,63}$/;
const NATIVE_UNAVAILABLE_REASONS = new Set(["source_not_retained", "source_failed", "invalid_bytes", "byte_limit", "lifecycle_ended"]);
const ARTIFACT_EVENT_TYPES = new Set(["artifact_published", "artifact_unavailable", "artifact_projection_limited"]);
const TERMINAL_FAILURE_CODES = new Set([
	"artifact_not_found",
	"artifact_unavailable",
	"source_failed",
	"byte_count_mismatch",
	"digest_mismatch",
	"invalid_limits",
	"quota_exceeded",
]);

function isRecord(value: unknown): value is Record<string, unknown> {
	return value !== null && typeof value === "object" && !Array.isArray(value);
}

function exactKeys(value: Record<string, unknown>, required: readonly string[], optional: readonly string[] = []): boolean {
	const allowed = new Set([...required, ...optional]);
	return required.every((key) => Object.hasOwn(value, key)) && Object.keys(value).every((key) => allowed.has(key));
}

function safeInteger(value: unknown): value is number {
	return Number.isSafeInteger(value) && (value as number) >= 0;
}

function safeBoundedText(value: unknown, maxBytes: number, allowControls = false): value is string {
	return typeof value === "string" && Buffer.byteLength(value) <= maxBytes && (allowControls || !/[\u0000-\u001f\u007f]/.test(value));
}

function safeMessage(value: unknown): value is string {
	return safeBoundedText(value, 512, true);
}

function safeTarget(value: unknown): value is WireTarget {
	return isRecord(value)
		&& exactKeys(value, ["stream_id", "session_id", "session_epoch"])
		&& safeBoundedText(value.stream_id, 128)
		&& isCanonicalUuid(value.session_id)
		&& safeInteger(value.session_epoch);
}

function sameIdentity(left: LiveIdentity, right: LiveIdentity): boolean {
	return left.streamId === right.streamId && left.sessionId === right.sessionId && left.sessionEpoch === right.sessionEpoch;
}

function wireTarget(target: LiveIdentity): WireTarget {
	return { stream_id: target.streamId, session_id: target.sessionId, session_epoch: target.sessionEpoch };
}

function identityFrom(value: LiveIdentity): LiveIdentity {
	return { streamId: value.streamId, sessionId: value.sessionId, sessionEpoch: value.sessionEpoch };
}

function parseProjection(value: unknown): NativeArtifactProjection {
	if (!isRecord(value)) throw new ManagedArtifactProtocolError("invalid_publication", "Native artifact metadata is invalid");
	const availability = value.availability;
	if (!isRecord(availability)) throw new ManagedArtifactProtocolError("invalid_publication", "Native artifact availability is invalid");
	let parsedAvailability: NativeArtifactAvailability;
	if (availability.status === "available" && exactKeys(availability, ["status"])) {
		parsedAvailability = { status: "available" };
	} else if (
		availability.status === "unavailable"
		&& exactKeys(availability, ["status", "reason", "message"])
		&& typeof availability.reason === "string"
		&& NATIVE_UNAVAILABLE_REASONS.has(availability.reason)
		&& safeMessage(availability.message)
	) {
		parsedAvailability = {
			status: "unavailable",
			reason: availability.reason as NativeArtifactAvailability["reason"],
			message: availability.message,
		};
	} else {
		throw new ManagedArtifactProtocolError("invalid_publication", "Native artifact availability is invalid");
	}
	if (
		typeof value.publicationId !== "string" || !PUBLICATION_PATTERN.test(value.publicationId)
		|| (value.kind !== "image" && value.kind !== "file")
		|| typeof value.displayName !== "string" || Buffer.byteLength(value.displayName) < 1 || Buffer.byteLength(value.displayName) > 255
		|| path.basename(value.displayName) !== value.displayName || /[\\/\u0000-\u001f\u007f]/.test(value.displayName)
		|| typeof value.mediaType !== "string" || Buffer.byteLength(value.mediaType) < 1 || Buffer.byteLength(value.mediaType) > 127
		|| /[^\x20-\x7e]/.test(value.mediaType)
		|| !safeInteger(value.byteLength) || (value.byteLength as number) > MANAGED_ARTIFACT_LIMITS.maxArtifactBytes
		|| (value.sha256 !== undefined && (typeof value.sha256 !== "string" || !DIGEST_PATTERN.test(value.sha256)))
		|| (value.disposition !== "inline" && value.disposition !== "attachment")
		|| (value.retention !== "session_inline" && value.retention !== "runtime_source")
		|| (value.origin !== "native" && value.origin !== "legacy_inline_image")
	) throw new ManagedArtifactProtocolError("invalid_publication", "Native artifact metadata is invalid");
	return {
		publicationId: value.publicationId,
		kind: value.kind,
		displayName: value.displayName,
		mediaType: value.mediaType,
		byteLength: value.byteLength as number,
		...(value.sha256 === undefined ? {} : { sha256: value.sha256 as string }),
		disposition: value.disposition,
		retention: value.retention,
		origin: value.origin,
		availability: parsedAvailability,
	};
}

function wirePublication(publication: NativeArtifactProjection): WirePublication {
	return {
		publication_id: publication.publicationId,
		kind: publication.kind,
		name: publication.displayName,
		media_type: publication.mediaType,
		size: publication.byteLength,
		sha256: publication.sha256 ?? null,
		disposition: publication.disposition,
		retention: publication.retention,
		origin: publication.origin,
		availability: publication.availability.status === "available"
			? { status: "available" }
			: {
				status: "unavailable",
				reason: publication.availability.reason!,
				message: publication.availability.message!,
			},
	};
}

function publicationFingerprint(publication: NativeArtifactProjection): string {
	return JSON.stringify(wirePublication(publication));
}

export function deriveManagedArtifactClientId(key: NativeManagedArtifactKey): string {
	if (!isCanonicalUuid(key.threadId) || !isCanonicalUuid(key.nativeSessionId) || !PUBLICATION_PATTERN.test(key.publicationId)) {
		throw new ManagedArtifactProtocolError("invalid_publication", "Managed artifact key is invalid");
	}
	const bytes = createHash("sha256")
		.update(`sesh.pi-artifact-map\0v1\0${key.threadId}\0${key.nativeSessionId}\0${key.publicationId}`, "utf8")
		.digest()
		.subarray(0, 16);
	bytes[6] = (bytes[6]! & 0x0f) | 0x80;
	bytes[8] = (bytes[8]! & 0x3f) | 0x80;
	const hex = bytes.toString("hex");
	return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function parseArtifactReference(value: unknown): ManagedArtifactReference {
	if (!isRecord(value) || !exactKeys(value, [
		"id", "thread_id", "target", "name", "media_type", "size", "sha256", "disposition", "expires_at_unix_ms",
	])) throw new ManagedArtifactProtocolError("invalid_response", "Sesh artifact reference has an invalid shape");
	if (
		!isCanonicalUuid(value.id) || !isCanonicalUuid(value.thread_id) || !safeTarget(value.target)
		|| typeof value.name !== "string" || Buffer.byteLength(value.name) < 1 || Buffer.byteLength(value.name) > 255
		|| path.basename(value.name) !== value.name || /[\\/\u0000-\u001f\u007f]/.test(value.name)
		|| typeof value.media_type !== "string" || Buffer.byteLength(value.media_type) < 1 || Buffer.byteLength(value.media_type) > 127
		|| /[^\x20-\x7e]/.test(value.media_type)
		|| !safeInteger(value.size) || typeof value.sha256 !== "string" || !DIGEST_PATTERN.test(value.sha256)
		|| (value.disposition !== "inline" && value.disposition !== "attachment")
		|| !safeInteger(value.expires_at_unix_ms)
	) throw new ManagedArtifactProtocolError("invalid_response", "Sesh artifact reference is invalid");
	return value as unknown as ManagedArtifactReference;
}

function parseRuntimeEnvelope(value: unknown): RuntimeEnvelope {
	if (!isRecord(value) || !exactKeys(value, ["schema", "protocol", "version", "mapping"])
		|| (value.schema !== 55 && value.schema !== 56)
		|| value.protocol !== "sesh.pi-artifact-map" || value.version !== 1 || !isRecord(value.mapping)) {
		throw new ManagedArtifactProtocolError("invalid_response", "Sesh mapping response envelope is invalid");
	}
	const mapping = value.mapping;
	if (!isCanonicalUuid(mapping.mapping_id) || typeof mapping.publication_id !== "string" || !PUBLICATION_PATTERN.test(mapping.publication_id)
		|| !safeInteger(mapping.updated_at_unix_ms)) {
		throw new ManagedArtifactProtocolError("invalid_response", "Sesh mapping response identity is invalid");
	}
	switch (mapping.status) {
		case "staging":
			if (!exactKeys(mapping, ["mapping_id", "publication_id", "status", "producer_target", "artifact_id", "updated_at_unix_ms"])
				|| !safeTarget(mapping.producer_target) || !isCanonicalUuid(mapping.artifact_id)) {
				throw new ManagedArtifactProtocolError("invalid_response", "Sesh staging mapping is invalid");
			}
			break;
		case "ready":
			if (!exactKeys(mapping, ["mapping_id", "publication_id", "status", "producer_target", "artifact", "updated_at_unix_ms"])
				|| !safeTarget(mapping.producer_target)) {
				throw new ManagedArtifactProtocolError("invalid_response", "Sesh ready mapping is invalid");
			}
			mapping.artifact = parseArtifactReference(mapping.artifact);
			break;
		case "unavailable":
			if (!exactKeys(mapping, ["mapping_id", "publication_id", "status", "unavailable", "updated_at_unix_ms"])
				|| !isRecord(mapping.unavailable) || !exactKeys(mapping.unavailable, ["source", "code", "message"])
				|| !new Set(["native", "native_copy", "sesh"]).has(mapping.unavailable.source as string)
				|| typeof mapping.unavailable.code !== "string" || !SAFE_ERROR_CODE_PATTERN.test(mapping.unavailable.code)
				|| !safeMessage(mapping.unavailable.message)) {
				throw new ManagedArtifactProtocolError("invalid_response", "Sesh unavailable mapping is invalid");
			}
			break;
		case "expired":
			if (!exactKeys(mapping, ["mapping_id", "publication_id", "status", "artifact_id", "expired_at_unix_ms", "updated_at_unix_ms"])
				|| !isCanonicalUuid(mapping.artifact_id) || !safeInteger(mapping.expired_at_unix_ms)) {
				throw new ManagedArtifactProtocolError("invalid_response", "Sesh expired mapping is invalid");
			}
			break;
		default:
			throw new ManagedArtifactProtocolError("invalid_response", "Sesh mapping response status is invalid");
	}
	return value as unknown as RuntimeEnvelope;
}

function parseErrorEnvelope(value: unknown, statusCode: number): ManagedArtifactProtocolError {
	if (!isRecord(value) || !exactKeys(value, ["schema", "status", "error"])
		|| (value.schema !== 55 && value.schema !== 56) || value.status !== "error"
		|| !isRecord(value.error) || !exactKeys(value.error, ["code", "message"])
		|| typeof value.error.code !== "string" || !SAFE_ERROR_CODE_PATTERN.test(value.error.code)
		|| !safeMessage(value.error.message)) {
		return new ManagedArtifactProtocolError("invalid_response", "Sesh error response is invalid", statusCode);
	}
	return new ManagedArtifactProtocolError(value.error.code, value.error.message, statusCode);
}

function parseJson(bytes: Buffer): unknown {
	const text = bytes.toString("utf8");
	const parsed = JSON.parse(text) as unknown;
	return parsed;
}

function requestBase(
	socketPath: string,
	method: "POST" | "PUT",
	requestPath: string,
	headers: Record<string, string | number>,
	timeoutMs: number,
	maxResponseBytes: number,
	signal: AbortSignal,
): { request: http.ClientRequest; response: Promise<{ statusCode: number; bytes: Buffer }> } {
	let responseResolve!: (value: { statusCode: number; bytes: Buffer }) => void;
	let responseReject!: (error: unknown) => void;
	const response = new Promise<{ statusCode: number; bytes: Buffer }>((resolve, reject) => {
		responseResolve = resolve;
		responseReject = reject;
	});
	const request = http.request({ socketPath, method, path: requestPath, headers }, (incoming) => {
		const chunks: Buffer[] = [];
		let size = 0;
		incoming.on("data", (chunk: Buffer) => {
			size += chunk.length;
			if (size > maxResponseBytes) {
				incoming.destroy(new ManagedArtifactProtocolError("invalid_response", "Sesh response exceeds its byte limit"));
				return;
			}
			chunks.push(chunk);
		});
		incoming.on("end", () => responseResolve({ statusCode: incoming.statusCode ?? 0, bytes: Buffer.concat(chunks, size) }));
		incoming.on("error", responseReject);
	});
	request.on("error", responseReject);
	const deadline = setTimeout(
		() => request.destroy(new ManagedArtifactProtocolError("timeout", "Sesh request timed out")),
		timeoutMs,
	);
	deadline.unref?.();
	const abort = () => request.destroy(new ManagedArtifactProtocolError("aborted", "Managed artifact publication was aborted"));
	if (signal.aborted) abort();
	else signal.addEventListener("abort", abort, { once: true });
	response.finally(() => {
		clearTimeout(deadline);
		signal.removeEventListener("abort", abort);
	}).catch(() => undefined);
	return { request, response };
}

async function parseHttpResponse(response: Promise<{ statusCode: number; bytes: Buffer }>): Promise<RuntimeEnvelope> {
	const { statusCode, bytes } = await response;
	let value: unknown;
	try {
		value = parseJson(bytes);
	} catch {
		throw new ManagedArtifactProtocolError("invalid_response", "Sesh response is not strict JSON", statusCode);
	}
	if (statusCode !== 200) throw parseErrorEnvelope(value, statusCode);
	return parseRuntimeEnvelope(value);
}

export function createManagedArtifactSeshClient(
	apiSocketPath: string,
	limits: Pick<ManagedArtifactPublisherLimits, "seshRequestTimeoutMs" | "maxResponseBytes"> = MANAGED_ARTIFACT_LIMITS,
): ManagedArtifactSeshClient {
	if (!path.isAbsolute(apiSocketPath) || /[\u0000-\u001f\u007f]/.test(apiSocketPath)) {
		throw new ManagedArtifactProtocolError("unsupported_runtime_transfer", "Sesh runtime socket identity is invalid");
	}
	const jsonRequest = async (method: "POST", requestPath: string, body: unknown, signal: AbortSignal): Promise<RuntimeEnvelope> => {
		const encoded = Buffer.from(JSON.stringify(body));
		const exchange = requestBase(apiSocketPath, method, requestPath, {
			"Content-Type": "application/json",
			"Content-Length": encoded.length,
		}, limits.seshRequestTimeoutMs, limits.maxResponseBytes, signal);
		exchange.request.end(encoded);
		return await parseHttpResponse(exchange.response);
	};
	return {
		resolveMapping(request, signal) {
			return jsonRequest("POST", "/v1/runtime/artifact-mappings/resolve", request, signal);
		},
		async putMappingContent(input, copy, signal) {
			const query = new URLSearchParams({ thread_id: input.threadId, mapping_id: input.mappingId }).toString();
			const exchange = requestBase(apiSocketPath, "PUT", `/v1/runtime/artifact-mappings/content?${query}`, {
				"Content-Type": "application/octet-stream",
				"Content-Length": input.contentLength,
				"X-Sesh-Consumer-Stream-ID": input.consumerTarget.stream_id,
				"X-Sesh-Consumer-Session-ID": input.consumerTarget.session_id,
				"X-Sesh-Consumer-Session-Epoch": input.consumerTarget.session_epoch,
			}, limits.seshRequestTimeoutMs, limits.maxResponseBytes, signal);
			let offset = 0;
			try {
				const copyResult = await copy(async (chunk, chunkOffset) => {
					if (chunkOffset !== offset || chunk.length < 1 || chunk.length > MANAGED_ARTIFACT_LIMITS.copyChunkBytes
						|| offset + chunk.length > input.contentLength) {
						throw new ManagedArtifactProtocolError("byte_count_mismatch", "Native artifact copy produced an invalid chunk sequence");
					}
					const accepted = exchange.request.write(Buffer.from(chunk));
					offset += chunk.length;
					if (!accepted) {
						await new Promise<void>((resolve, reject) => {
							const onDrain = () => { cleanup(); resolve(); };
							const onError = (error: unknown) => { cleanup(); reject(error); };
							const cleanup = () => {
								exchange.request.off("drain", onDrain);
								exchange.request.off("error", onError);
							};
							exchange.request.once("drain", onDrain);
							exchange.request.once("error", onError);
						});
					}
				});
				if (offset !== input.contentLength) {
					throw new ManagedArtifactProtocolError("byte_count_mismatch", "Native artifact copy did not produce the declared byte count");
				}
				exchange.request.end();
				const envelope = await parseHttpResponse(exchange.response);
				return { envelope, copyResult };
			} catch (error) {
				exchange.request.destroy();
				await exchange.response.catch(() => undefined);
				throw error;
			}
		},
		failMapping(request, signal) {
			return jsonRequest("POST", "/v1/runtime/artifact-mappings/fail", request, signal);
		},
	};
}

function failureMessage(code: ManagedArtifactFailRequest["failure"]["code"]): string {
	switch (code) {
		case "artifact_not_found": return "Pi artifact source was not found";
		case "artifact_unavailable": return "Pi artifact source is unavailable";
		case "source_failed": return "Pi artifact source failed during bounded copy";
		case "byte_count_mismatch": return "Artifact byte count did not match immutable metadata";
		case "digest_mismatch": return "Artifact digest did not match immutable metadata";
		case "invalid_limits": return "Artifact copy limits were rejected";
		case "quota_exceeded": return "Sesh managed artifact quota was exceeded";
	}
}

function structuralErrorCode(error: unknown): string | undefined {
	const code = isRecord(error) && typeof error.code === "string" ? error.code : undefined;
	return code && SAFE_ERROR_CODE_PATTERN.test(code) ? code : undefined;
}

function terminalFailure(error: unknown): { source: "native_copy" | "sesh"; code: ManagedArtifactFailRequest["failure"]["code"] } | undefined {
	const code = structuralErrorCode(error);
	if (!code || !TERMINAL_FAILURE_CODES.has(code)) return undefined;
	return {
		source: error instanceof ManagedArtifactProtocolError && error.statusCode !== 0 ? "sesh" : "native_copy",
		code: code as ManagedArtifactFailRequest["failure"]["code"],
	};
}

function validateReadyMapping(
	envelope: RuntimeEnvelope,
	publication: NativeArtifactProjection,
	threadId: string,
	nativeSessionId: string,
): void {
	const mapping = envelope.mapping;
	if (mapping.publication_id !== publication.publicationId) {
		throw new ManagedArtifactProtocolError("mapping_conflict", "Sesh mapping publication identity does not match");
	}
	const expectedId = deriveManagedArtifactClientId({ threadId, nativeSessionId, publicationId: publication.publicationId });
	if (mapping.mapping_id !== expectedId) {
		throw new ManagedArtifactProtocolError("mapping_conflict", "Sesh mapping identity does not match");
	}
	if (mapping.status === "staging" || mapping.status === "ready") {
		if (mapping.producer_target.session_id !== nativeSessionId) {
			throw new ManagedArtifactProtocolError("mapping_conflict", "Sesh producer target belongs to a different native session");
		}
	}
	if (mapping.status !== "ready") return;
	if (
		mapping.artifact.thread_id !== threadId
		|| mapping.artifact.target.stream_id !== mapping.producer_target.stream_id
		|| mapping.artifact.target.session_id !== mapping.producer_target.session_id
		|| mapping.artifact.target.session_epoch !== mapping.producer_target.session_epoch
		|| mapping.artifact.name !== publication.displayName
		|| mapping.artifact.media_type !== publication.mediaType
		|| mapping.artifact.size !== publication.byteLength
		|| mapping.artifact.sha256 !== publication.sha256
		|| mapping.artifact.disposition !== publication.disposition
	) throw new ManagedArtifactProtocolError("mapping_conflict", "Sesh ready artifact metadata does not match");
}

export async function startManagedArtifactPublisher(options: ManagedArtifactPublisherOptions): Promise<ManagedArtifactPublisher> {
	if (!isCanonicalUuid(options.threadId)) {
		throw new ManagedArtifactProtocolError("unsupported_runtime_transfer", "Sesh thread identity is invalid");
	}
	const limits: ManagedArtifactPublisherLimits = { ...MANAGED_ARTIFACT_LIMITS, ...options.limits };
	for (const [name, value] of Object.entries(limits)) {
		if (!Number.isSafeInteger(value) || value <= 0) throw new Error(`Invalid managed artifact limit: ${name}`);
	}
	if (limits.maxConcurrentCopies > 2 || limits.maxArtifactBytes > MANAGED_ARTIFACT_LIMITS.maxArtifactBytes
		|| limits.copyChunkBytes > MANAGED_ARTIFACT_LIMITS.copyChunkBytes) {
		throw new Error("Managed artifact limits exceed the published contract");
	}
	const client = options.client ?? createManagedArtifactSeshClient(options.apiSocketPath, limits);
	const onError = options.onError ?? (() => undefined);
	const rootAbort = new AbortController();
	let closed = false;
	let initializing = true;
	let target: LiveIdentity | undefined;
	let nativeSessionId = "";
	let active = 0;
	let reconciliationNeeded = true;
	let reconciling = false;
	let bufferedEventBytes = 0;
	const bufferedEvents: LiveEvent[] = [];
	const queue = new Map<string, NativeArtifactProjection>();
	const observed = new Map<string, { fingerprint: string; status: ManagedArtifactRuntimeMapping["status"] | "pending"; bytes: number }>();
	let observedBytes = 0;
	const activeTasks = new Set<Promise<void>>();

	const report = (_error: unknown, code = "managed_artifact_error") => {
		try { onError(new ManagedArtifactProtocolError(code, "Managed artifact publication failed")); } catch { /* reporting is isolated */ }
	};

	const remember = (publication: NativeArtifactProjection, status: ManagedArtifactRuntimeMapping["status"] | "pending"): boolean => {
		const fingerprint = publicationFingerprint(publication);
		const bytes = Buffer.byteLength(fingerprint) + 256;
		const existing = observed.get(publication.publicationId);
		if (existing) {
			if (existing.fingerprint === fingerprint) {
				existing.status = status;
				return true;
			}
			const previous = JSON.parse(existing.fingerprint) as WirePublication;
			const current = JSON.parse(fingerprint) as WirePublication;
			const onlyAvailabilityChanged = JSON.stringify({ ...previous, availability: undefined }) === JSON.stringify({ ...current, availability: undefined });
			if (!onlyAvailabilityChanged || previous.availability.status === "unavailable") {
				report(undefined, "mapping_conflict");
				return false;
			}
			observedBytes -= existing.bytes;
			if (observedBytes + bytes > limits.maxObservedMetadataBytes) {
				observedBytes += existing.bytes;
				return false;
			}
			existing.fingerprint = fingerprint;
			existing.bytes = bytes;
			existing.status = status;
			observedBytes += bytes;
			return true;
		}
		if (observed.size >= limits.maxObservedMappings || observedBytes + bytes > limits.maxObservedMetadataBytes) return false;
		observed.set(publication.publicationId, { fingerprint, status, bytes });
		observedBytes += bytes;
		return true;
	};

	const offer = (raw: unknown): void => {
		if (closed) return;
		let publication: NativeArtifactProjection;
		try { publication = parseProjection(raw); } catch (error) { report(error, "invalid_publication"); return; }
		const existing = observed.get(publication.publicationId);
		const fingerprint = publicationFingerprint(publication);
		if (existing?.fingerprint === fingerprint) return;
		if (queue.has(publication.publicationId)) {
			queue.set(publication.publicationId, publication);
			return;
		}
		if (queue.size + active >= limits.maxQueuedPublications) {
			reconciliationNeeded = true;
			return;
		}
		if (!existing && (observed.size >= limits.maxObservedMappings || observedBytes >= limits.maxObservedMetadataBytes)) return;
		queue.set(publication.publicationId, publication);
	};

	const recordTerminalFailure = async (
		publication: NativeArtifactProjection,
		failure: { source: "native_copy" | "sesh"; code: ManagedArtifactFailRequest["failure"]["code"] },
	): Promise<void> => {
		if (!target) return;
		try {
			const envelope = await client.failMapping({
				thread_id: options.threadId,
				consumer_target: wireTarget(target),
				native_session_id: nativeSessionId,
				publication_id: publication.publicationId,
				failure: { ...failure, message: failureMessage(failure.code) },
			}, rootAbort.signal);
			validateReadyMapping(envelope, publication, options.threadId, nativeSessionId);
			remember(publication, envelope.mapping.status);
		} catch (error) {
			report(error, structuralErrorCode(error) ?? "managed_artifact_fail_failed");
		}
	};

	const processPublication = async (publication: NativeArtifactProjection): Promise<void> => {
		if (!target || closed) return;
		if (!remember(publication, "pending")) return;
		try {
			const envelope = await client.resolveMapping({
				thread_id: options.threadId,
				consumer_target: wireTarget(target),
				native_session_id: nativeSessionId,
				publication: wirePublication(publication),
			}, rootAbort.signal);
			validateReadyMapping(envelope, publication, options.threadId, nativeSessionId);
			if (envelope.mapping.status !== "staging") {
				remember(publication, envelope.mapping.status);
				return;
			}
			if (publication.availability.status !== "available" || !publication.sha256) {
				throw new ManagedArtifactProtocolError("invalid_response", "Sesh returned staging for a non-copyable publication");
			}
			const content = await client.putMappingContent({
				threadId: options.threadId,
				mappingId: envelope.mapping.mapping_id,
				consumerTarget: wireTarget(target),
				contentLength: publication.byteLength,
			}, (write) => options.host.copyArtifactBytes(target!, publication.publicationId, {
				maxBytes: limits.maxArtifactBytes,
				chunkBytes: limits.copyChunkBytes,
				timeoutMs: limits.copyTimeoutMs,
				signal: rootAbort.signal,
			}, write), rootAbort.signal);
			if (content.copyResult.byteLength !== publication.byteLength) {
				await recordTerminalFailure(publication, { source: "native_copy", code: "byte_count_mismatch" });
				return;
			}
			if (content.copyResult.sha256 !== publication.sha256) {
				await recordTerminalFailure(publication, { source: "native_copy", code: "digest_mismatch" });
				return;
			}
			validateReadyMapping(content.envelope, publication, options.threadId, nativeSessionId);
			if (content.envelope.mapping.status !== "ready") {
				throw new ManagedArtifactProtocolError("invalid_response", "Sesh content response is not ready");
			}
			remember(publication, "ready");
		} catch (error) {
			if (rootAbort.signal.aborted) return;
			const terminal = terminalFailure(error);
			if (terminal) await recordTerminalFailure(publication, terminal);
			else report(error, structuralErrorCode(error) ?? "managed_artifact_retryable");
		}
	};

	const parsePage = (value: unknown): { artifacts: unknown[]; nextCursor?: { leafId: string | null; before: number } } => {
		if (!isRecord(value) || value.scope !== "current_branch_artifacts" || !Array.isArray(value.artifacts)
			|| !isRecord(value.coverage)) throw new ManagedArtifactProtocolError("invalid_response", "Native artifact page is invalid");
		let nextCursor: { leafId: string | null; before: number } | undefined;
		if (value.nextCursor !== undefined) {
			if (!isRecord(value.nextCursor) || !exactKeys(value.nextCursor, ["leafId", "before"])
				|| (value.nextCursor.leafId !== null && typeof value.nextCursor.leafId !== "string") || !safeInteger(value.nextCursor.before)) {
				throw new ManagedArtifactProtocolError("invalid_response", "Native artifact cursor is invalid");
			}
			nextCursor = { leafId: value.nextCursor.leafId as string | null, before: value.nextCursor.before as number };
		}
		return { artifacts: value.artifacts, ...(nextCursor ? { nextCursor } : {}) };
	};

	const reconcile = async (): Promise<void> => {
		if (closed || reconciling || !target) return;
		reconciling = true;
		reconciliationNeeded = false;
		try {
			const snapshot = options.host.snapshot() as LiveSnapshot & Record<string, unknown>;
			if (!sameIdentity(snapshot, target)) return;
			for (const artifact of Array.isArray(snapshot.pendingArtifacts) ? snapshot.pendingArtifacts : []) offer(artifact);
			for (const artifact of Array.isArray(snapshot.artifacts) ? snapshot.artifacts : []) offer(artifact);
			let cursor = isRecord(snapshot.artifactHistoryCursor)
				&& (snapshot.artifactHistoryCursor.leafId === null || typeof snapshot.artifactHistoryCursor.leafId === "string")
				&& safeInteger(snapshot.artifactHistoryCursor.before)
				? { leafId: snapshot.artifactHistoryCursor.leafId as string | null, before: snapshot.artifactHistoryCursor.before as number }
				: undefined;
			for (let pageIndex = 0; cursor && pageIndex < limits.maxReconciliationPages; pageIndex++) {
				if (queue.size + active >= limits.maxQueuedPublications || observed.size >= limits.maxObservedMappings) {
					reconciliationNeeded = observed.size < limits.maxObservedMappings;
					break;
				}
				const result = await options.host.dispatch(target, {
					type: "artifact_history",
					cursor,
					maxItems: Math.min(200, limits.maxObservedMappings - observed.size),
					maxBytes: 256 * 1024,
				}, () => undefined);
				const page = parsePage(result);
				for (const artifact of page.artifacts) offer(artifact);
				cursor = page.nextCursor;
			}
			if (cursor && observed.size < limits.maxObservedMappings) reconciliationNeeded = true;
		} catch (error) {
			if (!rootAbort.signal.aborted) report(error, "managed_artifact_reconciliation_failed");
		} finally {
			reconciling = false;
		}
	};

	const pump = (): void => {
		if (closed) return;
		while (active < limits.maxConcurrentCopies && queue.size > 0) {
			const first = queue.entries().next().value as [string, NativeArtifactProjection] | undefined;
			if (!first) break;
			queue.delete(first[0]);
			active++;
			const task = processPublication(first[1]).finally(() => {
				active--;
				activeTasks.delete(task);
				pump();
			});
			activeTasks.add(task);
		}
		if (active === 0 && queue.size === 0 && reconciliationNeeded && !reconciling) {
			const task = reconcile().finally(() => {
				activeTasks.delete(task);
				pump();
			});
			activeTasks.add(task);
		}
	};

	const handleEvent = (event: LiveEvent): void => {
		if (!target || !sameIdentity(event, target)) return;
		const record = event as LiveEvent & Record<string, unknown>;
		if (event.type === "artifact_published") offer(record.artifact);
		else if (event.type === "artifact_unavailable") {
			const publicationId = record.publicationId;
			const existing = typeof publicationId === "string" ? observed.get(publicationId) : undefined;
			if (existing && isRecord(record.availability)) {
				try {
					const previous = JSON.parse(existing.fingerprint) as WirePublication;
					offer(parseProjection({
						publicationId: previous.publication_id,
						kind: previous.kind,
						displayName: previous.name,
						mediaType: previous.media_type,
						byteLength: previous.size,
						...(previous.sha256 === null ? {} : { sha256: previous.sha256 }),
						disposition: previous.disposition,
						retention: previous.retention,
						origin: previous.origin,
						availability: record.availability,
					}));
				} catch { reconciliationNeeded = true; }
			} else reconciliationNeeded = true;
		} else if (event.type === "artifact_projection_limited") reconciliationNeeded = true;
		pump();
	};

	const unsubscribe = options.host.subscribe((event) => {
		if (!ARTIFACT_EVENT_TYPES.has(event.type)) return;
		if (!initializing) {
			handleEvent(event);
			return;
		}
		let bytes = 0;
		try { bytes = Buffer.byteLength(JSON.stringify(event)); } catch { reconciliationNeeded = true; return; }
		if (bufferedEvents.length >= limits.maxBufferedNativeEvents || bufferedEventBytes + bytes > limits.maxBufferedNativeEventBytes) {
			reconciliationNeeded = true;
			return;
		}
		bufferedEvents.push(event);
		bufferedEventBytes += bytes;
	});

	try {
		const snapshot = options.host.snapshot() as LiveSnapshot & Record<string, unknown>;
		target = identityFrom(snapshot);
		if (options.expectedTarget && !sameIdentity(target, options.expectedTarget)) {
			throw new ManagedArtifactProtocolError("stale_target", "Native target changed before managed publisher startup");
		}
		if (!isCanonicalUuid(target.sessionId)) throw new ManagedArtifactProtocolError("unsupported_runtime_transfer", "Native session identity is not canonical");
		nativeSessionId = target.sessionId;
		for (const artifact of Array.isArray(snapshot.pendingArtifacts) ? snapshot.pendingArtifacts : []) offer(artifact);
		for (const artifact of Array.isArray(snapshot.artifacts) ? snapshot.artifacts : []) offer(artifact);
	} catch (error) {
		unsubscribe();
		throw error;
	} finally {
		initializing = false;
	}
	for (const event of bufferedEvents.splice(0)) handleEvent(event);
	bufferedEventBytes = 0;
	pump();

	return {
		capability: MANAGED_ARTIFACT_CAPABILITY,
		async close() {
			if (closed) return;
			closed = true;
			unsubscribe();
			rootAbort.abort();
			queue.clear();
			bufferedEvents.length = 0;
			await Promise.allSettled([...activeTasks]);
		},
	};
}
