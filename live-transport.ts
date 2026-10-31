import * as net from "node:net";
import * as fs from "node:fs";
import * as path from "node:path";
import { inputImageSocketPath, liveSocketPath } from "./socket-paths.ts";
import { validateLiveWireCommand } from "./live-wire-validation.ts";
import {
	AttachmentTransferError,
	initializeLiveAttachmentConsumer,
	initializeLiveTransferMarker,
	isCanonicalUuid,
	type LiveAttachmentConsumer,
	type LiveAttachmentConsumerOptions,
	type LiveDispatchAttachment,
	type LiveTransferMarker,
} from "./attachment-transfer.ts";
import {
	startManagedArtifactPublisher,
	type ArtifactCapableLiveTransportHost,
	type ManagedArtifactPublisher,
	type ManagedArtifactPublisherOptions,
} from "./managed-artifact-publisher.ts";
import {
	startLiveInputImageReader,
	type CurrentBranchInputImageCapableHost,
	type LiveInputImageReader,
} from "./input-image-reader.ts";

export const LIVE_PROTOCOL = "live-v1" as const;

export interface LiveIdentity {
	streamId: string;
	sessionEpoch: number;
	sessionId: string;
}

export interface LiveSnapshot extends LiveIdentity {
	sequence: number;
}

export interface LiveEvent extends LiveIdentity {
	sequence: number;
	type: string;
}

/**
 * Out-of-band native dispatch options. The socket creates them itself; they never
 * come from `live-v1`, an `RpcCommand`, a retained result or a Sesh field.
 */
export interface LiveHostDispatchOptions {
	imageAdmission?: "strict-v1";
}

/** Structural seam implemented by Pi's public LiveRpcSession. */
export interface LiveTransportHost {
	snapshot(): LiveSnapshot;
	subscribe(listener: (event: LiveEvent) => void): () => void;
	dispatch(
		target: LiveIdentity,
		command: unknown,
		output: (response: unknown) => void,
		options?: LiveHostDispatchOptions,
	): Promise<unknown | undefined>;
	/** Detach this observer only. It must not dispose the owning runtime. */
	close(): void;
}

/**
 * Pi's exact seven-field image-admission capability. Detection requires exactly these
 * own enumerable keys with exactly these literal values.
 */
export interface LiveImageAdmissionCapability {
	protocol: "pi.image-admission";
	version: 1;
	preparation: "rpc-prompt-files-strict-v1";
	dispatch: "standard-user-strict-v1";
	requestValidation: "normalized-messages-v1";
	virtualModels: "stable-profile-v1";
	binaryInLive: false;
}

/**
 * Pi's exact six-field interactive-input capability (`pi.interactive-input`). It says
 * the attached host can INTERPRET a raw input line the way the TUI's own submit path
 * does — builtin dispatch first, then prompt — and nothing more. Detection requires
 * exactly these own enumerable keys with exactly these literal values.
 *
 * Absence is legitimate: a non-interactive runtime, a host without the operation, or
 * a host whose interpreter is not wired. Support is never inferred from a Pi/package
 * version, from `interactive_builtin`, from a pending interaction, from observed
 * events, or by submitting a line and reading the error.
 */
export interface LiveInteractiveInputCapability {
	protocol: "pi.interactive-input";
	version: 1;
	input: "interactive-input-v1";
	literal: "expand-prompt-templates-v1";
	busy: "steer-default-v1";
	bash: "not-remote";
}

export interface LiveInputImageCapability {
	protocol: "pi.input-images";
	version: 1;
	messageBlock: "input_image_reference";
	metadataQuery: "input_image_history";
	maxImageBytes: 33554432;
	copyChunkBytes: 262144;
	maxPendingImages: 256;
	maxPendingDeclaredBytes: 268435456;
	maxPendingMetadataBytes: 262144;
	binaryInLive: false;
}

export interface LiveOutputArtifactPresentationCapability {
	protocol: "pi.output-artifact-presentation";
	version: 1;
	messageBlock: "output_artifact_reference";
	metadataQuery: "artifact_history";
	coverage: "direct_tool_result_content";
	states: readonly ["resolved", "partial", "missing", "duplicate", "unsupported"];
	nestedToolResults: "unsupported_placeholder";
	binaryInLive: false;
}

export interface CopyInputImageBytesOptions {
	maxBytes: number;
	chunkBytes?: number;
	timeoutMs?: number;
	signal?: AbortSignal;
}

export interface CopyInputImageBytesResult {
	byteLength: number;
	sha256: string;
}

export type InputImageChunkWriter = (chunk: Uint8Array, offset: number) => void | Promise<void>;

/** Complete public direct-metadata and byte-copy seam supplied by Pi's live input-image host. */
export interface InputImageCapableLiveTransportHost extends CurrentBranchInputImageCapableHost {}

export interface LiveTransportLimits {
	maxClients: number;
	maxInputBytes: number;
	maxFrameBytes: number;
	maxClientQueueFrames: number;
	maxClientQueueBytes: number;
	maxReplayEvents: number;
	maxReplayBytes: number;
	maxResults: number;
	maxResultBytes: number;
	maxConcurrentQueries: number;
	resultRetentionMs: number;
	idleClientTimeoutMs: number;
}

const DEFAULT_LIMITS: LiveTransportLimits = {
	maxClients: 16,
	maxInputBytes: 1024 * 1024,
	maxFrameBytes: 1024 * 1024,
	maxClientQueueFrames: 512,
	maxClientQueueBytes: 1024 * 1024,
	maxReplayEvents: 2048,
	maxReplayBytes: 8 * 1024 * 1024,
	maxResults: 256,
	maxResultBytes: 8 * 1024 * 1024,
	maxConcurrentQueries: 16,
	resultRetentionMs: 60 * 60 * 1000,
	idleClientTimeoutMs: 30_000,
};

export interface LiveManagedArtifactOptions extends Omit<ManagedArtifactPublisherOptions, "host" | "onError" | "expectedTarget"> {
	inboxPath: string;
}

export interface LiveInputImageReaderOptions {
	threadId: string;
	inboxPath: string;
	apiSocketPath: string;
}

export interface StartLiveTransportOptions {
	socketPath: string;
	inputImageSocketPath?: string;
	limits?: Partial<LiveTransportLimits>;
	now?: () => number;
	onError?: (error: unknown) => void;
	attachments?: LiveAttachmentConsumerOptions;
	managedArtifacts?: LiveManagedArtifactOptions;
	inputImageReader?: LiveInputImageReaderOptions;
}

export interface StartLiveSessionTransportOptions extends Omit<StartLiveTransportOptions, "socketPath" | "inputImageSocketPath"> {
	socketsDir?: string;
}

export interface LiveTransportServer {
	readonly socketPath: string;
	close(): Promise<void>;
}

type ErrorBody = { code: string; message: string };
type StoredResult =
	| { requestId: string; status: "running" }
	| { requestId: string; status: "completed"; response: unknown; finishedAt: number }
	| { requestId: string; status: "failed"; error: ErrorBody; finishedAt: number }
	/**
	 * A strict image dispatch the socket actually handed to the host, which rejected
	 * before any accepted or result settlement with something that is neither an exact
	 * allowlisted admission rejection nor an authoritative stale-target/runtime refusal.
	 * `finishedAt` timestamps that host rejection, not agent completion or confirmed
	 * admission. Delivery of this record succeeded; the submission remains unresolved.
	 */
	| { requestId: string; status: "uncertain"; reason: "native_dispatch_rejected"; finishedAt: number }
	| {
			requestId: string;
			status: "unavailable";
			outcome: "completed";
			reason: "response_unserializable" | "response_too_large" | "retention_budget";
			finishedAt: number;
	  };

type ResultRecord = {
	target: LiveIdentity;
	resultJson: string;
	status: StoredResult["status"];
	finishedAt?: number;
	retainedBytes: number;
};

type ReplayEntry = { event: LiveEvent; encodedBytes: number };

type Client = {
	socket: net.Socket;
	input: Buffer;
	queue: string[];
	queueBytes: number;
	blocked: boolean;
	closed: boolean;
	subscribed: boolean;
	subscriptionId?: string;
	idleTimer?: NodeJS.Timeout;
};

class WireError extends Error {
	constructor(readonly code: string, message: string) {
		super(message);
	}
}

class FrameTooLargeError extends Error {}

const ID_PATTERN = /^[A-Za-z0-9_-]{1,128}$/;
const RESULT_ENTRY_RESERVE_BYTES = 4096;
const ALLOWED_OPS = new Set(["capabilities", "subscribe", "snapshot", "query", "dispatch", "get_result"]);
const BASE_QUERY_COMMAND_TYPES = new Set([
	"history",
	"branch_history",
	"get_state",
	"get_available_models",
	"get_available_thinking_levels",
	"get_session_stats",
	"get_fork_messages",
	"get_entries",
	"get_tree",
	"get_last_assistant_text",
	"get_messages",
	"get_commands",
]);
const TRANSCRIPT_CAPABILITIES = {
	snapshot: "bounded-with-coverage",
	history: { direction: "older", maxMessages: 500, maxBytes: 768 * 1024 },
	branchHistory: { direction: "older", maxItems: 500, maxBytes: 768 * 1024 },
} as const;

function isRecord(value: unknown): value is Record<string, unknown> {
	return value !== null && typeof value === "object" && !Array.isArray(value);
}

function hasExactKeys(value: Record<string, unknown>, required: readonly string[], optional: readonly string[] = []): boolean {
	const allowed = new Set([...required, ...optional]);
	return Object.keys(value).every((key) => allowed.has(key)) && required.every((key) => Object.hasOwn(value, key));
}

function isBoundedIdentityText(value: unknown, maxBytes = 512): value is string {
	return typeof value === "string" && Buffer.byteLength(value, "utf8") >= 1
		&& Buffer.byteLength(value, "utf8") <= maxBytes && !/[\u0000-\u001f\u007f]/.test(value);
}

function isNonNegativeSafeInteger(value: unknown): value is number {
	return Number.isSafeInteger(value) && (value as number) >= 0;
}

function isInputImageCursor(value: unknown): boolean {
	return isRecord(value) && hasExactKeys(value, ["leafId", "before"])
		&& (value.leafId === null || isBoundedIdentityText(value.leafId))
		&& isNonNegativeSafeInteger(value.before);
}

function isInputImageCoverage(value: unknown): boolean {
	if (!isRecord(value) || !hasExactKeys(value, [
		"scope", "leafId", "startIndex", "endIndexExclusive", "totalInputImages", "byteLength", "hasOlder", "hasNewer",
	], ["oversizedInputImage"])) return false;
	if (value.scope !== "current_branch_input_images" || (value.leafId !== null && !isBoundedIdentityText(value.leafId))) return false;
	if (!isNonNegativeSafeInteger(value.startIndex) || !isNonNegativeSafeInteger(value.endIndexExclusive)
		|| !isNonNegativeSafeInteger(value.totalInputImages) || !isNonNegativeSafeInteger(value.byteLength)
		|| value.byteLength < 2 || value.startIndex > value.endIndexExclusive
		|| value.endIndexExclusive > value.totalInputImages || typeof value.hasOlder !== "boolean"
		|| typeof value.hasNewer !== "boolean") return false;
	if (value.oversizedInputImage === undefined) return true;
	const oversized = value.oversizedInputImage;
	return isRecord(oversized) && hasExactKeys(oversized, ["index", "inputImageId", "byteLength"])
		&& isNonNegativeSafeInteger(oversized.index) && isBoundedIdentityText(oversized.inputImageId, 128)
		&& isNonNegativeSafeInteger(oversized.byteLength);
}

function detectInputImageCapability(host: LiveTransportHost, snapshot: LiveSnapshot): LiveInputImageCapability | undefined {
	const inputHost = host as Partial<InputImageCapableLiveTransportHost>;
	if (typeof inputHost.getCurrentBranchInputImage !== "function" || typeof inputHost.copyInputImageBytes !== "function") return undefined;
	const candidate = snapshot as LiveSnapshot & Record<string, unknown>;
	const capability = candidate.inputImageCapability;
	if (!isRecord(capability) || !hasExactKeys(capability, [
		"protocol", "version", "messageBlock", "metadataQuery", "maxImageBytes", "copyChunkBytes",
		"maxPendingImages", "maxPendingDeclaredBytes", "maxPendingMetadataBytes", "binaryInLive",
	])) return undefined;
	if (capability.protocol !== "pi.input-images" || capability.version !== 1
		|| capability.messageBlock !== "input_image_reference" || capability.metadataQuery !== "input_image_history"
		|| capability.maxImageBytes !== 33554432 || capability.copyChunkBytes !== 262144
		|| capability.maxPendingImages !== 256 || capability.maxPendingDeclaredBytes !== 268435456
		|| capability.maxPendingMetadataBytes !== 262144 || capability.binaryInLive !== false) return undefined;
	if (!Array.isArray(candidate.inputImages) || !Array.isArray(candidate.pendingInputImages)
		|| !isInputImageCoverage(candidate.inputImageCoverage)
		|| (candidate.inputImageHistoryCursor !== undefined && !isInputImageCursor(candidate.inputImageHistoryCursor))) return undefined;
	return {
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
}

const IMAGE_ADMISSION_CAPABILITY_FIELDS = [
	"protocol",
	"version",
	"preparation",
	"dispatch",
	"requestValidation",
	"virtualModels",
	"binaryInLive",
] as const;

const VIEW_RESOURCE_CAPABILITY_FIELDS = [
	"protocol",
	"version",
	"read",
	"staleness",
	"retention",
] as const;

const INTERACTIVE_INPUT_CAPABILITY_FIELDS = [
	"protocol",
	"version",
	"input",
	"literal",
	"busy",
	"bash",
] as const;

/**
 * What a runtime must advertise before `view_resource_read` is served.
 *
 * Mirrored field for field from the host's own `LIVE_RPC_VIEW_RESOURCE_CAPABILITY`:
 * `read` names the read operation, `staleness` the binding-staleness rule, and
 * `retention` the fact that a page goes to the requesting caller only and is never
 * published, replayed, cached or recorded as a command result. All are literal — a
 * runtime with different semantics is a different contract, not a configurable one.
 *
 * The 32 KiB page ceiling is deliberately NOT part of this record: the host does not
 * advertise it, so the socket must not invent a field claiming it does. The ceiling
 * lives in the wire validator, which clamps `maxBytes` down to it.
 */
export interface LiveViewResourceCapability {
	readonly protocol: "pi.view-resources";
	readonly version: 1;
	readonly read: "view-resource-read-v1";
	readonly staleness: "binding-stale-v1";
	readonly retention: "requester-only";
}

/**
 * Exact-validate one `pi.view-resources` capability record and return a detached
 * frozen copy. Nothing else implies support: not the presence of a view controller,
 * not a declared resource, not an observed view event, not a Pi version. A runtime
 * without this record simply does not get `view_resource_read` in its allowlist, and
 * every other live-v1 surface is unaffected.
 */
export function detectLiveViewResourceCapability(value: unknown): LiveViewResourceCapability | undefined {
	if (!isRecord(value) || !hasExactKeys(value, VIEW_RESOURCE_CAPABILITY_FIELDS)) return undefined;
	if (value.protocol !== "pi.view-resources" || value.version !== 1
		|| value.read !== "view-resource-read-v1" || value.staleness !== "binding-stale-v1"
		|| value.retention !== "requester-only") return undefined;
	return Object.freeze({
		protocol: "pi.view-resources",
		version: 1,
		read: "view-resource-read-v1",
		staleness: "binding-stale-v1",
		retention: "requester-only",
	});
}

/**
 * The five-member result union a view-resource read may settle as, checked before the
 * socket forwards it.
 *
 * This is a FORWARDING guard, not a re-classification: a recognised result is passed
 * through byte for byte, including the `error` prose on `invalid`. The socket adds no
 * sixth status and rewrites none of the five.
 *
 * `read_timed_out` is NOT a status. The host returns
 * `{status:"invalid", error:"The view resource handler did not finish within the read
 * deadline."}` — one of twelve diagnostics delivered under the single `invalid`
 * status. Which is exactly why `error` must survive the hop: `invalid` alone cannot
 * tell a consumer whether re-offering the read is worth anything.
 */
function isViewResourceResult(value: unknown): boolean {
	if (!isRecord(value)) return false;
	switch (value.status) {
		case "page":
			return hasExactKeys(value, ["status", "page"]) && isRecord(value.page);
		case "stale":
			return hasExactKeys(value, ["status", "revision", "datasetId"])
				&& Number.isSafeInteger(value.revision) && (value.revision as number) >= 0
				&& typeof value.datasetId === "string" && value.datasetId.length > 0;
		case "not_found":
		case "already_resolved":
			return hasExactKeys(value, ["status"]);
		case "invalid":
			return hasExactKeys(value, ["status", "error"]) && typeof value.error === "string";
		default:
			return false;
	}
}

/**
 * Exact-validate one `pi.interactive-input` capability record and return a detached
 * frozen copy. The socket mirrors the host's record; it never synthesises, defaults
 * or normalises a field, and it never claims the capability on its own behalf. A
 * missing, malformed or differently-valued record simply yields `undefined`, which
 * means "this runtime does not interpret input remotely".
 */
export function detectLiveInteractiveInputCapability(
	value: unknown,
): LiveInteractiveInputCapability | undefined {
	if (!isRecord(value) || !hasExactKeys(value, INTERACTIVE_INPUT_CAPABILITY_FIELDS)) return undefined;
	if (value.protocol !== "pi.interactive-input" || value.version !== 1
		|| value.input !== "interactive-input-v1" || value.literal !== "expand-prompt-templates-v1"
		|| value.busy !== "steer-default-v1" || value.bash !== "not-remote") return undefined;
	return Object.freeze({
		protocol: "pi.interactive-input",
		version: 1,
		input: "interactive-input-v1",
		literal: "expand-prompt-templates-v1",
		busy: "steer-default-v1",
		bash: "not-remote",
	});
}

/**
 * Exact-validate one `pi.image-admission` capability record and return a detached
 * frozen copy. Nothing else implies support: not helper presence, function arity, a
 * Pi/package version, `pi.input-images`, observed image events, a selected model, a
 * virtual profile, output-v2, managed output, a Sesh schema, the environment, the
 * hostname or a runtime mode. Obsolete broad capabilities carrying classifier,
 * format, decoder, source-limit, declared-MIME, unchanged-byte or payload-validation
 * fields are rejected by the exact key set.
 */
export function detectLiveImageAdmissionCapability(value: unknown): LiveImageAdmissionCapability | undefined {
	if (!isRecord(value) || !hasExactKeys(value, IMAGE_ADMISSION_CAPABILITY_FIELDS)) return undefined;
	if (value.protocol !== "pi.image-admission" || value.version !== 1
		|| value.preparation !== "rpc-prompt-files-strict-v1" || value.dispatch !== "standard-user-strict-v1"
		|| value.requestValidation !== "normalized-messages-v1" || value.virtualModels !== "stable-profile-v1"
		|| value.binaryInLive !== false) return undefined;
	return Object.freeze({
		protocol: "pi.image-admission",
		version: 1,
		preparation: "rpc-prompt-files-strict-v1",
		dispatch: "standard-user-strict-v1",
		requestValidation: "normalized-messages-v1",
		virtualModels: "stable-profile-v1",
		binaryInLive: false,
	});
}

/**
 * Field-for-field equality of two already detached capability records. The socket
 * never resnapshots to make an imported and a snapshot capability agree, and never
 * combines a capability from one target or generation with another.
 */
function sameImageAdmissionCapability(
	left: LiveImageAdmissionCapability,
	right: LiveImageAdmissionCapability,
): boolean {
	return IMAGE_ADMISSION_CAPABILITY_FIELDS.every((field) => left[field] === right[field]);
}

const OUTPUT_ARTIFACT_PRESENTATION_STATES = [
	"resolved",
	"partial",
	"missing",
	"duplicate",
	"unsupported",
] as const;

function detectOutputArtifactPresentationCapability(
	snapshot: LiveSnapshot,
): LiveOutputArtifactPresentationCapability | undefined {
	const candidate = snapshot as LiveSnapshot & Record<string, unknown>;
	const capability = candidate.outputArtifactPresentationCapability;
	if (!isRecord(capability) || !hasExactKeys(capability, [
		"protocol", "version", "messageBlock", "metadataQuery", "coverage", "states", "nestedToolResults", "binaryInLive",
	])) return undefined;
	if (capability.protocol !== "pi.output-artifact-presentation" || capability.version !== 1
		|| capability.messageBlock !== "output_artifact_reference" || capability.metadataQuery !== "artifact_history"
		|| capability.coverage !== "direct_tool_result_content"
		|| capability.nestedToolResults !== "unsupported_placeholder" || capability.binaryInLive !== false) return undefined;
	const states = capability.states;
	if (!Array.isArray(states) || states.length !== OUTPUT_ARTIFACT_PRESENTATION_STATES.length
		|| !OUTPUT_ARTIFACT_PRESENTATION_STATES.every((state, index) => states[index] === state)) return undefined;
	return {
		protocol: "pi.output-artifact-presentation",
		version: 1,
		messageBlock: "output_artifact_reference",
		metadataQuery: "artifact_history",
		coverage: "direct_tool_result_content",
		states: [...OUTPUT_ARTIFACT_PRESENTATION_STATES],
		nestedToolResults: "unsupported_placeholder",
		binaryInLive: false,
	};
}

function assertExactKeys(value: Record<string, unknown>, required: string[], optional: string[] = []): void {
	const allowed = new Set([...required, ...optional]);
	for (const key of Object.keys(value)) {
		if (!allowed.has(key)) throw new WireError("invalid_request", `unknown field: ${key}`);
	}
	for (const key of required) {
		if (!Object.hasOwn(value, key)) throw new WireError("invalid_request", `missing field: ${key}`);
	}
}

function assertId(value: unknown, field = "id"): asserts value is string {
	if (typeof value !== "string" || !ID_PATTERN.test(value)) {
		throw new WireError("invalid_request", `${field} must be 1-128 ASCII letters, digits, _ or -`);
	}
}

function assertBoundedText(value: unknown, field: string, max: number): asserts value is string {
	if (typeof value !== "string" || value.length === 0 || value.length > max || /[\u0000-\u001f\u007f]/.test(value)) {
		throw new WireError("invalid_request", `${field} must be non-empty text without control characters (max ${max})`);
	}
}

function assertNonNegativeInteger(value: unknown, field: string): asserts value is number {
	if (!Number.isSafeInteger(value) || (value as number) < 0) {
		throw new WireError("invalid_request", `${field} must be a non-negative safe integer`);
	}
}

function parseIdentity(value: unknown, field = "target"): LiveIdentity {
	if (!isRecord(value)) throw new WireError("invalid_request", `${field} must be an object`);
	assertExactKeys(value, ["streamId", "sessionEpoch", "sessionId"]);
	assertBoundedText(value.streamId, `${field}.streamId`, 128);
	assertNonNegativeInteger(value.sessionEpoch, `${field}.sessionEpoch`);
	assertBoundedText(value.sessionId, `${field}.sessionId`, 512);
	return {
		streamId: value.streamId,
		sessionEpoch: value.sessionEpoch,
		sessionId: value.sessionId,
	};
}

function parseAttachments(value: unknown): LiveDispatchAttachment[] {
	if (!Array.isArray(value) || value.length < 1 || value.length > 8) {
		throw new WireError("invalid_request", "attachments must contain 1-8 lease references");
	}
	const seen = new Set<string>();
	return value.map((entry, index) => {
		if (!isRecord(entry)) throw new WireError("invalid_request", `attachments[${index}] must be an object`);
		assertExactKeys(entry, ["leaseId"]);
		if (!isCanonicalUuid(entry.leaseId)) {
			throw new WireError("invalid_request", `attachments[${index}].leaseId must be a canonical UUID`);
		}
		if (seen.has(entry.leaseId)) throw new WireError("invalid_request", "attachment lease IDs must be unique");
		seen.add(entry.leaseId);
		return { leaseId: entry.leaseId };
	});
}


function validateRequest(value: unknown, queryCommandTypes: ReadonlySet<string>): Record<string, unknown> {
	if (!isRecord(value)) throw new WireError("invalid_request", "request must be an object");
	if (value.protocol !== LIVE_PROTOCOL) throw new WireError("invalid_request", `protocol must be ${LIVE_PROTOCOL}`);
	assertId(value.id);
	if (typeof value.op !== "string" || !ALLOWED_OPS.has(value.op)) {
		throw new WireError("invalid_request", "unsupported operation");
	}

	switch (value.op) {
		case "capabilities":
		case "snapshot":
			assertExactKeys(value, ["protocol", "id", "op"]);
			break;
		case "subscribe": {
			assertExactKeys(value, ["protocol", "id", "op"], ["after"]);
			if (value.after !== undefined) {
				if (!isRecord(value.after)) throw new WireError("invalid_request", "after must be an object");
				assertExactKeys(value.after, ["streamId", "sequence"]);
				assertBoundedText(value.after.streamId, "after.streamId", 128);
				assertNonNegativeInteger(value.after.sequence, "after.sequence");
			}
			break;
		}
		case "query":
		case "dispatch": {
			assertExactKeys(value, ["protocol", "id", "op", "target", "command"], value.op === "dispatch" ? ["attachments"] : []);
			parseIdentity(value.target);
			if (!isRecord(value.command)) throw new WireError("invalid_request", "command must be an object");
			validateLiveWireCommand(value.command, (message) => {
				throw new WireError("invalid_request", message);
			});
			if (value.op === "query" && !queryCommandTypes.has(value.command.type as string)) {
				throw new WireError("invalid_request", `command type is not allowed for query: ${String(value.command.type)}`);
			}
			if (value.op === "dispatch" && value.attachments !== undefined) {
				parseAttachments(value.attachments);
				if (!isCanonicalUuid(value.id)) throw new WireError("invalid_request", "attachment dispatch id must be a canonical UUID");
				if (!new Set(["prompt", "steer", "follow_up"]).has(value.command.type as string)) {
					throw new WireError("invalid_request", "attachments are allowed only for prompt, steer, or follow_up");
				}
				// Lease-backed preparation merges prepared images with any existing
				// `command.images`, so caller-supplied images would bypass the lease-only
				// transfer boundary. Refuse the field itself, before mutation reservation,
				// lease reads, scratch creation, helper invocation and native dispatch.
				// Ordinary non-attachment `command.images` behavior is unchanged.
				if (Object.hasOwn(value.command, "images")) {
					throw new WireError("invalid_request", "attachment dispatch must not carry command.images");
				}
			}
			break;
		}
		case "get_result":
			assertExactKeys(value, ["protocol", "id", "op", "target", "requestId"]);
			parseIdentity(value.target);
			assertId(value.requestId, "requestId");
			break;
	}
	return value;
}

/**
 * Pi's thirteen admission-phase codes. A rejection before ownership leaves no Pi-owned
 * standard user message, queue insertion, input-image reference, provider call or
 * success response, so these are definitive rather than uncertain.
 */
const NATIVE_IMAGE_ADMISSION_CODES: ReadonlySet<string> = new Set([
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
]);

/**
 * Pi's five request-phase codes. This is a must-not-map guard, never a mapping table:
 * phase `request` happens after accepted queue/run ownership and is reported through
 * normal Pi run events. A synchronous request-phase rejection breaches the native
 * contract and stays unresolved instead of becoming an admission rejection.
 */
const NATIVE_IMAGE_REQUEST_CODES: ReadonlySet<string> = new Set([
	"image_input_blocked",
	"image_model_unsupported",
	"image_virtual_route_incompatible",
	"image_message_limit_exceeded",
	"image_request_limit_exceeded",
]);

/**
 * The complete set of authoritative refusals a standard-user strict dispatch can
 * actually raise, matched by exact whole-message equality.
 *
 * Every key is a literal copied from Pi's source: the dispatch and strict-authority
 * assertions throw `Live RPC target is stale; reconcile the session before submitting`
 * (`modes/rpc/live-rpc-session.ts:445,645`), and the ready-state checks throw
 * `Runtime is ${state}` for the non-ready members of
 * `AgentSessionRuntimeState = "ready" | "replacing" | "failed" | "disposed"`
 * (`core/agent-session-runtime.ts:67,142,182,500`; `modes/rpc/live-rpc-session.ts:346,612`).
 *
 * Artifact-copy and input-image-copy staleness have their own distinct wording and are
 * deliberately absent: they cannot arise from a prompt, steer or follow-up dispatch.
 * `message` is socket-owned fixed text, and `label` is a socket-owned diagnostic label.
 * Neither is derived from the error, so no native prose can reach a frame or a log.
 */
const STRICT_DISPATCH_REFUSALS: ReadonlyMap<string, { code: string; message: string; label: string }> = new Map([
	["Live RPC target is stale; reconcile the session before submitting", {
		code: "stale_target",
		message: "Live dispatch target is stale; reconcile the session before resubmitting",
		label: "stale target",
	}],
	["Runtime is replacing", {
		code: "runtime_unavailable",
		message: "Live runtime is unavailable; reconcile the session before resubmitting",
		label: "runtime replacing",
	}],
	["Runtime is failed", {
		code: "runtime_unavailable",
		message: "Live runtime is unavailable; reconcile the session before resubmitting",
		label: "runtime failed",
	}],
	["Runtime is disposed", {
		code: "runtime_unavailable",
		message: "Live runtime is unavailable; reconcile the session before resubmitting",
		label: "runtime disposed",
	}],
]);

type StrictRejectionClassification = {
	/** Present only for a definitively no-effect rejection; always fixed socket text. */
	definitive?: ErrorBody;
	/** Socket-owned label for a matched refusal, for local diagnostics only. */
	label?: string;
	/** An allowlisted request-phase code that breached the dispatch contract. */
	requestPhaseCode?: string;
};

/**
 * Classify a rejection from a dispatch this socket gave its own strict fourth argument.
 *
 * Only two things are definitive: an exact native `{phase:"admission", code}` pair, and
 * an exact whole-message refusal from the allowlist above. Everything else stays
 * unresolved — unknown prose that merely contains or resembles a refusal phrase, a
 * request-phase or otherwise malformed typed error (even when its own message matches a
 * refusal), a non-`Error` value, or a property read that throws. The rejected value is
 * never stringified, serialized, pattern-matched loosely or echoed here, and a throwing
 * accessor cannot escape through a retention-error diagnostic.
 */
function classifyStrictDispatchRejection(error: unknown): StrictRejectionClassification {
	try {
		if (!(error instanceof Error)) return {};
		const typed = error as Error & { phase?: unknown; code?: unknown };
		const phase = typed.phase;
		const code = typed.code;
		if (phase !== undefined || code !== undefined) {
			// A typed native error: its phase/code decide, never its prose.
			if (phase === "admission" && typeof code === "string" && NATIVE_IMAGE_ADMISSION_CODES.has(code)) {
				return {
					definitive: { code: "image_admission_rejected", message: `Native image admission rejected (${code})` },
				};
			}
			if (phase === "request" && typeof code === "string" && NATIVE_IMAGE_REQUEST_CODES.has(code)) {
				return { requestPhaseCode: code };
			}
			return {};
		}
		const message = typed.message;
		if (typeof message !== "string") return {};
		const refusal = STRICT_DISPATCH_REFUSALS.get(message);
		return refusal === undefined
			? {}
			: { definitive: { code: refusal.code, message: refusal.message }, label: refusal.label };
	} catch {
		// A failing accessor is a conservatively unresolved outcome, not a definitive one.
		return {};
	}
}

function safeError(error: unknown): ErrorBody {
	const boundedMessage = (message: string) => message.length <= 512 ? message : `${message.slice(0, 511)}…`;
	if (error instanceof WireError) return { code: error.code, message: boundedMessage(error.message) };
	if (error instanceof AttachmentTransferError) return { code: error.code, message: boundedMessage(error.message) };
	const message = boundedMessage(error instanceof Error ? error.message : String(error));
	if (/target is stale/i.test(message)) return { code: "stale_target", message };
	if (/Runtime is (replacing|failed|disposed)/i.test(message)) return { code: "runtime_unavailable", message };
	return { code: "internal_error", message };
}

const INTERACTION_LIFECYCLE_EVENT_TYPES: ReadonlySet<string> = new Set([
	"interaction_opened",
	"interaction_updated",
	"interaction_resolved",
]);

/**
 * Exact kind-scoped test for an extension-view interaction lifecycle event. It reads two
 * literal fields — the event type and `interaction.kind` — and never walks, copies or
 * inspects extension-authored title, state, action or result data. Every other
 * interaction kind keeps its existing replay behaviour, and a lifecycle event without an
 * exact `extension_view` interaction is not treated as one.
 */
function isExtensionViewInteractionEvent(event: LiveEvent): boolean {
	if (!INTERACTION_LIFECYCLE_EVENT_TYPES.has(event.type)) return false;
	const interaction = (event as LiveEvent & { interaction?: unknown }).interaction;
	return isRecord(interaction) && interaction.kind === "extension_view";
}

/** A bounded, syntax-checked event type for fixed diagnostics; never payload text. */
function eventTypeLabel(event: LiveEvent): string {
	return typeof event.type === "string" && /^[A-Za-z0-9_.:-]{1,64}$/.test(event.type) ? event.type : "unknown";
}

function sameIdentity(left: LiveIdentity, right: LiveIdentity): boolean {
	return (
		left.streamId === right.streamId &&
		left.sessionEpoch === right.sessionEpoch &&
		left.sessionId === right.sessionId
	);
}

function ensureLimits(overrides?: Partial<LiveTransportLimits>): LiveTransportLimits {
	const limits = { ...DEFAULT_LIMITS, ...overrides };
	for (const [name, value] of Object.entries(limits)) {
		if (!Number.isSafeInteger(value) || value <= 0) throw new Error(`Invalid live transport limit ${name}`);
	}
	if (limits.maxResultBytes < limits.maxResults * RESULT_ENTRY_RESERVE_BYTES) {
		throw new Error(
			`Invalid live transport limit maxResultBytes: must be at least maxResults * ${RESULT_ENTRY_RESERVE_BYTES}`,
		);
	}
	return limits;
}

export type SocketProbeResult = "active" | "stale";
export type SocketConnector = (socketPath: string) => net.Socket;

/** Probe without guessing: only a successful connection or confirmed stale errno is decisive. */
export async function probeSocketPath(
	socketPath: string,
	timeoutMs = 250,
	connect: SocketConnector = (candidate) => net.createConnection(candidate),
): Promise<SocketProbeResult> {
	return await new Promise<SocketProbeResult>((resolve, reject) => {
		const socket = connect(socketPath);
		let settled = false;
		const finish = (result: SocketProbeResult) => {
			if (settled) return;
			settled = true;
			socket.destroy();
			resolve(result);
		};
		const fail = (error: Error) => {
			if (settled) return;
			settled = true;
			socket.destroy();
			reject(error);
		};
		socket.once("connect", () => finish("active"));
		socket.once("error", (error: NodeJS.ErrnoException) => {
			if (error.code === "ECONNREFUSED" || error.code === "ENOENT") {
				finish("stale");
				return;
			}
			fail(new Error(`Cannot safely classify existing Unix socket ${socketPath}: ${error.code ?? error.message}`, { cause: error }));
		});
		socket.setTimeout(timeoutMs, () => {
			fail(new Error(`Timed out probing existing Unix socket ${socketPath}; refusing to unlink it`));
		});
	});
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
	if (!stat.isSocket()) throw new Error(`Refusing to replace non-socket path: ${socketPath}`);
	const probe = await probeSocketPath(socketPath);
	if (probe === "active") throw new Error(`Live RPC socket is already active: ${socketPath}`);
	try {
		await fs.promises.unlink(socketPath);
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
	}
}

export async function startLiveTransport(
	host: LiveTransportHost,
	options: StartLiveTransportOptions,
): Promise<LiveTransportServer> {
	const limits = ensureLimits(options.limits);
	const now = options.now ?? Date.now;
	const onError = options.onError ?? ((error: unknown) => console.error("Live transport error:", error));
	const clients = new Set<Client>();
	const results = new Map<string, ResultRecord>();
	const activeQueries = new Set<string>();
	const replay: ReplayEntry[] = [];
	let replayBytes = 0;
	let replayFloor = 0;
	let resultBytes = 0;
	let closed = false;
	let closing: Promise<void> | undefined;
	let ownedSocket: { dev: number; ino: number } | undefined;

	let queryCommandTypes = new Set(BASE_QUERY_COMMAND_TYPES);
	let inputImageCapability: LiveInputImageCapability | undefined;
	let interactiveInputCapability: LiveInteractiveInputCapability | undefined;
	let viewResourceCapability: LiveViewResourceCapability | undefined;
	let outputArtifactPresentationCapability: LiveOutputArtifactPresentationCapability | undefined;
	let attachmentConsumer: LiveAttachmentConsumer | undefined;
	let managedArtifactPublisher: ManagedArtifactPublisher | undefined;
	let transferMarker: LiveTransferMarker | undefined;
	let inputImageReader: LiveInputImageReader | undefined;
	const reportError = (error: unknown) => {
		try {
			onError(error);
		} catch (reportError) {
			console.error("Live transport error handler failed:", reportError, "Original error:", error);
		}
	};

	const encode = (frame: unknown): string => {
		let encoded: string;
		try {
			encoded = JSON.stringify(frame) + "\n";
		} catch {
			// Fixed text only. A serialization exception names property paths inside the
			// value being encoded, which can be extension-authored or otherwise private.
			throw new WireError("internal_error", "response is not serializable");
		}
		if (Buffer.byteLength(encoded) > limits.maxFrameBytes) throw new FrameTooLargeError("frame exceeds outbound limit");
		return encoded;
	};

	const destroyClient = (client: Client, error?: Error) => {
		if (client.closed) return;
		client.closed = true;
		if (client.idleTimer) clearTimeout(client.idleTimer);
		clients.delete(client);
		client.socket.destroy(error);
	};

	const flush = (client: Client) => {
		if (client.closed || client.blocked) return;
		while (client.queue.length > 0) {
			const encoded = client.queue.shift()!;
			client.queueBytes -= Buffer.byteLength(encoded);
			if (!client.socket.write(encoded)) {
				client.blocked = true;
				return;
			}
		}
	};

	const sendEncoded = (client: Client, encoded: string) => {
		if (client.closed) return;
		const bytes = Buffer.byteLength(encoded);
		if (
			client.queue.length + 1 > limits.maxClientQueueFrames ||
			client.queueBytes + bytes + client.socket.writableLength > limits.maxClientQueueBytes
		) {
			destroyClient(client, new Error("live client exceeded backpressure limit"));
			return;
		}
		client.queue.push(encoded);
		client.queueBytes += bytes;
		flush(client);
	};

	const send = (client: Client, frame: unknown) => sendEncoded(client, encode(frame));
	const response = (client: Client, id: string, result: unknown) =>
		send(client, { protocol: LIVE_PROTOCOL, type: "response", id, ok: true, result });
	const responseError = (client: Client, id: string | undefined, error: unknown) => {
		const body = safeError(error);
		try {
			send(client, { protocol: LIVE_PROTOCOL, type: "response", ...(id ? { id } : {}), ok: false, error: body });
		} catch {
			destroyClient(client);
		}
	};
	const endWithError = (client: Client, error: unknown) => {
		if (client.closed) return;
		const body = safeError(error);
		client.closed = true;
		if (client.idleTimer) clearTimeout(client.idleTimer);
		clients.delete(client);
		try {
			client.socket.end(encode({ protocol: LIVE_PROTOCOL, type: "response", ok: false, error: body }));
		} catch {
			client.socket.destroy();
		}
	};

	const snapshotFrame = (client: Client, snapshot: LiveSnapshot) => {
		send(client, {
			protocol: LIVE_PROTOCOL,
			type: "snapshot",
			subscriptionId: client.subscriptionId,
			snapshot,
		});
	};

	const eventFrame = (client: Client, event: LiveEvent) => {
		send(client, {
			protocol: LIVE_PROTOCOL,
			type: "event",
			subscriptionId: client.subscriptionId,
			event,
		});
	};

	const gapFrame = (
		client: Client,
		afterSequence: number,
		availableFromSequence: number,
		reason: string,
	) => {
		send(client, {
			protocol: LIVE_PROTOCOL,
			type: "gap",
			subscriptionId: client.subscriptionId,
			gap: { afterSequence, availableFromSequence, reason },
		});
	};

	const resnapshotAfterOversizedEvent = (client: Client, event: LiveEvent) => {
		try {
			gapFrame(client, event.sequence - 1, event.sequence + 1, "event_too_large");
			const snapshot = host.snapshot();
			snapshotFrame(client, snapshot);
		} catch (error) {
			responseError(client, undefined, error instanceof FrameTooLargeError
				? new WireError("frame_too_large", "snapshot exceeds outbound frame limit")
				: error);
			destroyClient(client);
		}
	};

	const barrierReplay = (sequence: number) => {
		replay.length = 0;
		replayBytes = 0;
		replayFloor = sequence;
	};

	const appendReplay = (event: LiveEvent) => {
		// Two event families are delivered to already-attached observers but never
		// retained, and their sequence becomes a replay barrier so no later replay can
		// skip across the omitted event or recover its payload from transport memory:
		//
		//  - native auth notifications, which carry OAuth URLs, device codes and manual
		//    instructions;
		//  - extension-view interaction lifecycle events, whose interaction carries
		//    extension-authored title and state that only the current authoritative
		//    snapshot may serve after a reconnect.
		//
		// Events are never rewritten at a stored position; a barriered event is simply
		// not retained.
		if (event.type === "auth_notification" || isExtensionViewInteractionEvent(event)) {
			barrierReplay(event.sequence);
			return;
		}
		let bytes: number;
		try {
			bytes = Buffer.byteLength(JSON.stringify(event));
			encode({
				protocol: LIVE_PROTOCOL,
				type: "event",
				subscriptionId: "x".repeat(128),
				event,
			});
		} catch (error) {
			barrierReplay(event.sequence);
			// Fixed diagnostic: a serialization failure must not echo the thrown
			// exception, which names property paths inside the event payload.
			if (!(error instanceof FrameTooLargeError)) {
				reportError(new Error(
					`Live event ${eventTypeLabel(event)} at sequence ${event.sequence} could not be serialized for replay`,
				));
			}
			return;
		}
		if (bytes > limits.maxReplayBytes) {
			replay.length = 0;
			replayBytes = 0;
			replayFloor = event.sequence;
			return;
		}
		replay.push({ event, encodedBytes: bytes });
		replayBytes += bytes;
		while (replay.length > limits.maxReplayEvents || replayBytes > limits.maxReplayBytes) {
			const removed = replay.shift()!;
			replayBytes -= removed.encodedBytes;
			replayFloor = removed.event.sequence;
		}
	};

	const unsubscribe = host.subscribe((event) => {
		try {
			appendReplay(event);
			for (const client of clients) {
				if (!client.subscribed) continue;
				try {
					eventFrame(client, event);
				} catch (error) {
					if (error instanceof FrameTooLargeError) resnapshotAfterOversizedEvent(client, event);
					else {
						reportError(error);
						destroyClient(client);
					}
				}
			}
		} catch (error) {
			reportError(error);
		}
	});
	let hostDetached = false;
	const detachHost = () => {
		if (hostDetached) return;
		hostDetached = true;
		unsubscribe();
		try {
			host.close();
		} catch (error) {
			reportError(error);
		}
	};

	const pruneResults = () => {
		for (const [id, record] of results) {
			if (record.status !== "running" && record.finishedAt !== undefined && now() - record.finishedAt >= limits.resultRetentionMs) {
				results.delete(id);
				resultBytes -= record.retainedBytes;
			}
		}
	};

	const handleSubscribe = (client: Client, request: Record<string, unknown>) => {
		if (client.subscribed) throw new WireError("invalid_request", "connection is already subscribed");
		const snapshot = host.snapshot();
		client.subscriptionId = request.id as string;
		if (client.idleTimer) {
			clearTimeout(client.idleTimer);
			client.idleTimer = undefined;
		}

		const sendEventsAfter = (sequence: number) => {
			for (const entry of replay) {
				if (
					entry.event.streamId === snapshot.streamId &&
					entry.event.sequence > sequence
				) eventFrame(client, entry.event);
			}
		};
		let encodedSnapshot: string;
		try {
			encodedSnapshot = encode({
				protocol: LIVE_PROTOCOL,
				type: "snapshot",
				subscriptionId: client.subscriptionId,
				snapshot,
			});
		} catch (error) {
			throw error instanceof FrameTooLargeError
				? new WireError("frame_too_large", "snapshot exceeds outbound frame limit")
				: error;
		}
		const sendSnapshot = () => {
			sendEncoded(client, encodedSnapshot);
			sendEventsAfter(snapshot.sequence);
		};

		const after = request.after as Record<string, unknown> | undefined;
		if (!after) {
			response(client, request.id as string, { subscriptionId: client.subscriptionId, mode: "snapshot" });
			sendSnapshot();
			client.subscribed = true;
			return;
		}

		const afterStream = after.streamId as string;
		const afterSequence = after.sequence as number;
		const availableFrom = replay.length > 0 ? replay[0].event.sequence : snapshot.sequence + 1;
		const canReplay =
			afterStream === snapshot.streamId &&
			afterSequence <= snapshot.sequence &&
			afterSequence >= replayFloor &&
			(
				afterSequence === snapshot.sequence ||
				(replay.length > 0 && replay[0].event.sequence <= afterSequence + 1)
			);

		if (canReplay) {
			response(client, request.id as string, { subscriptionId: client.subscriptionId, mode: "replay" });
			sendEventsAfter(afterSequence);
			client.subscribed = true;
			return;
		}

		let reason = "replay_unavailable";
		if (afterStream !== snapshot.streamId) reason = "stream_changed";
		else if (afterSequence > snapshot.sequence) reason = "sequence_ahead";
		response(client, request.id as string, { subscriptionId: client.subscriptionId, mode: "snapshot" });
		gapFrame(client, afterSequence, availableFrom, reason);
		sendSnapshot();
		client.subscribed = true;
	};

	const serializeResultRecord = (target: LiveIdentity, result: StoredResult) => {
		const targetJson = JSON.stringify(target);
		const resultJson = JSON.stringify(result);
		const retainedBytes = Math.max(
			RESULT_ENTRY_RESERVE_BYTES,
			Buffer.byteLength(`{"target":${targetJson},"result":${resultJson}}`),
		);
		return { resultJson, retainedBytes };
	};

	const replaceStoredResult = (record: ResultRecord, result: StoredResult): boolean => {
		let serialized: ReturnType<typeof serializeResultRecord>;
		try {
			serialized = serializeResultRecord(record.target, result);
		} catch {
			return false;
		}
		if (resultBytes - record.retainedBytes + serialized.retainedBytes > limits.maxResultBytes) return false;
		resultBytes += serialized.retainedBytes - record.retainedBytes;
		record.resultJson = serialized.resultJson;
		record.status = result.status;
		record.finishedAt = result.status === "running" ? undefined : result.finishedAt;
		record.retainedBytes = serialized.retainedBytes;
		return true;
	};

	const unavailableResult = (
		requestId: string,
		reason: Extract<StoredResult, { status: "unavailable" }>["reason"],
	): Extract<StoredResult, { status: "unavailable" }> => ({
		requestId,
		status: "unavailable",
		outcome: "completed",
		reason,
		finishedAt: now(),
	});

	const complete = (requestId: string, responseValue: unknown): StoredResult => {
		const record = results.get(requestId);
		if (!record) throw new Error(`Missing live result record for ${requestId}`);
		const completed: Extract<StoredResult, { status: "completed" }> = {
			requestId,
			status: "completed",
			response: responseValue,
			finishedAt: now(),
		};
		let unavailableReason: Extract<StoredResult, { status: "unavailable" }>["reason"] | undefined;
		try {
			encode({ protocol: LIVE_PROTOCOL, type: "response", id: requestId, ok: true, result: completed });
		} catch (error) {
			unavailableReason = error instanceof FrameTooLargeError ? "response_too_large" : "response_unserializable";
		}
		if (!unavailableReason && replaceStoredResult(record, completed)) return completed;
		unavailableReason ??= "retention_budget";
		const unavailable = unavailableResult(requestId, unavailableReason);
		if (!replaceStoredResult(record, unavailable)) {
			throw new Error(`Unable to retain completion tombstone for ${requestId}`);
		}
		return unavailable;
	};

	const uncertainResult = (requestId: string): Extract<StoredResult, { status: "uncertain" }> => ({
		requestId,
		status: "uncertain",
		reason: "native_dispatch_rejected",
		finishedAt: now(),
	});

	/**
	 * Retain the exact four-field uncertain record. Its bytes are part of the upfront
	 * dispatch reservation, so retention cannot fail under result-capacity pressure.
	 */
	const retainUncertain = (requestId: string): Extract<StoredResult, { status: "uncertain" }> => {
		const record = results.get(requestId);
		if (!record) throw new Error(`Missing live result record for ${requestId}`);
		const result = uncertainResult(requestId);
		if (!replaceStoredResult(record, result)) throw new Error(`Unable to retain uncertain result for ${requestId}`);
		return result;
	};

	/**
	 * `definitive` is supplied only for a strict image dispatch the socket itself
	 * classified; that body is already fixed safe text, so the legacy `safeError`
	 * classification and its bounded echo never run for it.
	 */
	const fail = (
		requestId: string,
		error: unknown,
		definitive?: ErrorBody,
	): Extract<StoredResult, { status: "failed" }> => {
		const record = results.get(requestId);
		if (!record) throw new Error(`Missing live result record for ${requestId}`);
		let result: Extract<StoredResult, { status: "failed" }> = {
			requestId,
			status: "failed",
			error: definitive ?? safeError(error),
			finishedAt: now(),
		};
		if (!replaceStoredResult(record, result)) {
			result = {
				requestId,
				status: "failed",
				error: { code: result.error.code, message: "Command failed; detailed error unavailable" },
				finishedAt: result.finishedAt,
			};
			if (!replaceStoredResult(record, result)) throw new Error(`Unable to retain failure record for ${requestId}`);
		}
		return result;
	};

	const handleDispatch = (client: Client, request: Record<string, unknown>) => {
		const requestId = request.id as string;
		const target = parseIdentity(request.target);
		pruneResults();
		if (results.has(requestId) || activeQueries.has(requestId)) {
			throw new WireError("duplicate_request", "request id already exists; use get_result instead of resending");
		}
		if (results.size >= limits.maxResults) throw new WireError("capacity", `result capacity reached (${limits.maxResults})`);
		const running: StoredResult = { requestId, status: "running" };
		const runningSerialized = serializeResultRecord(target, running);
		const tombstoneBytes = Math.max(
			runningSerialized.retainedBytes,
			serializeResultRecord(target, unavailableResult(requestId, "response_unserializable")).retainedBytes,
			serializeResultRecord(target, unavailableResult(requestId, "response_too_large")).retainedBytes,
			serializeResultRecord(target, unavailableResult(requestId, "retention_budget")).retainedBytes,
			serializeResultRecord(target, uncertainResult(requestId)).retainedBytes,
		);
		if (resultBytes + tombstoneBytes > limits.maxResultBytes) {
			throw new WireError("capacity", `result byte capacity reached (${limits.maxResultBytes})`);
		}
		results.set(requestId, {
			target,
			resultJson: runningSerialized.resultJson,
			status: "running",
			retainedBytes: tombstoneBytes,
		});
		resultBytes += tombstoneBytes;
		let settled = false;
		const settleResponse = (value: unknown) => {
			if (settled) {
				reportError(new Error(`Live host produced more than one result for ${requestId}`));
				return;
			}
			settled = true;
			try {
				const result = complete(requestId, value);
				if (!client.closed) response(client, requestId, result);
			} catch (error) {
				reportError(error);
				if (!client.closed) responseError(client, requestId, error);
			}
		};
		const attachmentReferences = request.attachments === undefined ? undefined : parseAttachments(request.attachments);
		// True only once this socket has actually handed the host its own strict fourth
		// argument for this request. Preparation failures never reach strict handling.
		let strictImageDispatch = false;
		void (async () => {
			let command = request.command as Record<string, unknown>;
			if (attachmentReferences) {
				if (!attachmentConsumer) throw new AttachmentTransferError("transfer_unavailable", "Attachment leases are unavailable for this runtime");
				const prepared = await attachmentConsumer.prepare(requestId, target, command, attachmentReferences);
				command = prepared.command;
				if (prepared.strictImageAdmission) {
					strictImageDispatch = true;
					return await host.dispatch(target, command, settleResponse, { imageAdmission: "strict-v1" });
				}
			}
			return await host.dispatch(target, command, settleResponse);
		})().then(
			(value) => {
				if (value !== undefined) settleResponse(value);
			},
			(error) => {
				if (settled) {
					// An accepted result is never rewritten, and strict diagnostics never
					// echo raw native exception text.
					reportError(strictImageDispatch
						? new Error(`Strict image dispatch rejected after settlement for ${requestId}`)
						: error);
					return;
				}
				settled = true;
				try {
					// One guarded classification per strict rejection. Non-strict dispatch
					// keeps the legacy `safeError` path untouched.
					const strict = strictImageDispatch ? classifyStrictDispatchRejection(error) : {};
					if (strictImageDispatch && strict.definitive === undefined) {
						reportError(new Error(strict.requestPhaseCode === undefined
							? `Strict image dispatch for ${requestId} rejected with an unrecognized error; the submission is unresolved`
							: `Strict image dispatch for ${requestId} rejected synchronously with request-phase ${strict.requestPhaseCode}; the submission is unresolved`));
						const result = retainUncertain(requestId);
						if (!client.closed) response(client, requestId, result);
						return;
					}
					if (strict.label !== undefined) {
						reportError(new Error(`Strict image dispatch for ${requestId} was refused: ${strict.label}`));
					}
					const result = fail(requestId, error, strict.definitive);
					if (!client.closed) responseError(client, requestId, new WireError(result.error.code, result.error.message));
				} catch (retentionError) {
					reportError(retentionError);
					if (!client.closed) responseError(client, requestId, retentionError);
				}
			},
		);
	};

	const handleQuery = (client: Client, request: Record<string, unknown>) => {
		const requestId = request.id as string;
		// A view-resource read is the one query whose RESULT shape the socket checks.
		// Every other query returns whatever its host command returns; this one has a
		// fixed five-member union, and a page is bytes a viewer will render, so an
		// unrecognised shape fails closed to the requester instead of being forwarded
		// as a partial page. Recognised results pass through untouched.
		const isViewResourceRead =
			(request.command as Record<string, unknown> | undefined)?.type === "view_resource_read";
		if (activeQueries.has(requestId) || results.has(requestId)) {
			throw new WireError("duplicate_request", "request id is already active or retained");
		}
		if (activeQueries.size >= limits.maxConcurrentQueries) {
			throw new WireError("capacity", `concurrent query capacity reached (${limits.maxConcurrentQueries})`);
		}
		activeQueries.add(requestId);
		let settled = false;
		const settle = (value: unknown) => {
			if (settled) {
				reportError(new Error(`Live host produced more than one query result for ${requestId}`));
				return;
			}
			settled = true;
			activeQueries.delete(requestId);
			if (isViewResourceRead && !isViewResourceResult(value)) {
				reportError(new Error(`Live host returned a malformed view-resource result for ${requestId}`));
				if (!client.closed) {
					responseError(client, requestId, new WireError("internal_error", "view resource result is not one of the five contract statuses"));
				}
				return;
			}
			try {
				if (!client.closed) response(client, requestId, value);
			} catch (error) {
				const framedError = error instanceof FrameTooLargeError
					? new WireError("frame_too_large", "query result exceeds outbound frame limit")
					: error;
				if (!client.closed) responseError(client, requestId, framedError);
			}
		};
		void host.dispatch(parseIdentity(request.target), request.command, settle).then(
			(value) => {
				if (value === undefined) {
					if (!settled) {
						settled = true;
						activeQueries.delete(requestId);
						if (!client.closed) responseError(client, requestId, new WireError("internal_error", "query produced no response"));
					}
					return;
				}
				settle(value);
			},
			(error) => {
				if (settled) {
					reportError(error);
					return;
				}
				settled = true;
				activeQueries.delete(requestId);
				if (!client.closed) responseError(client, requestId, error);
			},
		);
	};

	const handleRequest = (client: Client, raw: unknown) => {
		let request: Record<string, unknown>;
		try {
			request = validateRequest(raw, queryCommandTypes);
		} catch (error) {
			const responseId = isRecord(raw) && typeof raw.id === "string" && ID_PATTERN.test(raw.id)
				? raw.id
				: undefined;
			responseError(client, responseId, error);
			return;
		}
		try {
			switch (request.op) {
				case "capabilities":
					response(client, request.id as string, {
						protocol: LIVE_PROTOCOL,
						operations: [...ALLOWED_OPS],
						limits,
						resultStates: ["running", "completed", "failed", "unavailable", "uncertain"],
						queryCommandTypes: [...queryCommandTypes],
						remoteAuth: "external",
						transcript: TRANSCRIPT_CAPABILITIES,
						...(interactiveInputCapability ? { interactiveInput: interactiveInputCapability } : {}),
						...(viewResourceCapability ? { viewResources: viewResourceCapability } : {}),
						...(inputImageCapability ? { inputImages: inputImageCapability } : {}),
						...(outputArtifactPresentationCapability ? { outputArtifactPresentation: outputArtifactPresentationCapability } : {}),
						...(inputImageReader ? { inputImageBytes: inputImageReader.capability } : {}),
						...(attachmentConsumer ? { attachments: attachmentConsumer.capability } : {}),
						...(managedArtifactPublisher ? { managedArtifacts: managedArtifactPublisher.capability } : {}),
					});
					break;
				case "snapshot":
					response(client, request.id as string, { snapshot: host.snapshot() });
					break;
				case "subscribe":
					handleSubscribe(client, request);
					break;
				case "query":
					handleQuery(client, request);
					break;
				case "dispatch":
					handleDispatch(client, request);
					break;
				case "get_result": {
					pruneResults();
					const record = results.get(request.requestId as string);
					if (!record) throw new WireError("result_not_found", "unknown or expired result; do not automatically retry an uncertain dispatch");
					if (!sameIdentity(record.target, parseIdentity(request.target))) {
						throw new WireError("result_target_mismatch", "result belongs to a different original target");
					}
					response(client, request.id as string, JSON.parse(record.resultJson) as StoredResult);
					break;
				}
			}
		} catch (error) {
			responseError(client, request.id as string, error);
		}
	};

	try {
		await prepareSocket(options.socketPath);
	} catch (error) {
		detachHost();
		throw error;
	}
	const transferTarget = host.snapshot();
	inputImageCapability = detectInputImageCapability(host, transferTarget);
	// Mirrored from the SAME startup snapshot as the other capabilities; the socket
	// never resnapshots to make a capability appear, and never combines a record
	// from one target or generation with another.
	interactiveInputCapability = detectLiveInteractiveInputCapability(
		(transferTarget as LiveSnapshot & Record<string, unknown>).interactiveInputCapability,
	);
	outputArtifactPresentationCapability = detectOutputArtifactPresentationCapability(transferTarget);
	// Same startup snapshot as every other capability. A runtime without the exact
	// record does not get `view_resource_read` in its allowlist at all, so an
	// unsupported host refuses the command at `validateRequest` with
	// `unknown_command` rather than dispatching a read it cannot serve.
	viewResourceCapability = detectLiveViewResourceCapability(
		(transferTarget as LiveSnapshot & Record<string, unknown>).viewResourceCapability,
	);
	if (inputImageCapability) queryCommandTypes.add("input_image_history");
	if (outputArtifactPresentationCapability) queryCommandTypes.add("artifact_history");
	if (viewResourceCapability) queryCommandTypes.add("view_resource_read");
	// Strict native preparation support: the capability imported from the same module
	// instance as `prepareRpcPromptFiles` must exist, the same startup snapshot must
	// carry one, and the two detached records must be equal field for field. A missing,
	// malformed, obsolete or mismatched capability simply leaves it false; protocol 2,
	// ordinary live-v1, text attachments, input-image projection and reads, managed
	// output and output-v2 are unaffected.
	const importedImageAdmissionCapability = detectLiveImageAdmissionCapability(
		options.attachments?.importedImageAdmissionCapability,
	);
	const snapshotImageAdmissionCapability = detectLiveImageAdmissionCapability(
		(transferTarget as LiveSnapshot & Record<string, unknown>).imageAdmissionCapability,
	);
	const strictImagePreparation = importedImageAdmissionCapability !== undefined
		&& snapshotImageAdmissionCapability !== undefined
		&& sameImageAdmissionCapability(importedImageAdmissionCapability, snapshotImageAdmissionCapability);
	if (options.attachments) {
		try {
			attachmentConsumer = await initializeLiveAttachmentConsumer(
				{ ...options.attachments, markerOwner: "external", strictImagePreparation },
				transferTarget,
				path.dirname(options.socketPath),
				now,
			);
		} catch (error) {
			reportError(error);
		}
	}
	const artifactHost = host as ArtifactCapableLiveTransportHost;
	const inputHost = host as InputImageCapableLiveTransportHost;
	let managedRequested = options.managedArtifacts !== undefined && typeof artifactHost.copyArtifactBytes === "function";
	let inputReaderRequested = options.inputImageReader !== undefined && inputImageCapability !== undefined;
	const sameTransferOptions = (
		left: { threadId: string; inboxPath: string; apiSocketPath: string },
		right: { threadId: string; inboxPath: string; apiSocketPath: string },
	) => left.threadId === right.threadId && left.inboxPath === right.inboxPath && left.apiSocketPath === right.apiSocketPath;
	const markerOptions = attachmentConsumer ? options.attachments : managedRequested ? options.managedArtifacts
		: inputReaderRequested ? options.inputImageReader : undefined;
	if (markerOptions && managedRequested && options.managedArtifacts && !sameTransferOptions(markerOptions, options.managedArtifacts)) {
		reportError(new Error("Managed artifacts require the same exact Sesh transfer identity as the other consumers"));
		managedRequested = false;
	}
	if (markerOptions && inputReaderRequested && options.inputImageReader && !sameTransferOptions(markerOptions, options.inputImageReader)) {
		reportError(new Error("Input-image reads require the same exact Sesh transfer identity as the other consumers"));
		inputReaderRequested = false;
	}
	if (inputReaderRequested && options.inputImageReader) {
		try {
			inputImageReader = await startLiveInputImageReader(inputHost, {
				threadId: options.inputImageReader.threadId,
				target: transferTarget,
				socketPath: options.inputImageSocketPath ?? path.join(path.dirname(options.socketPath), `${transferTarget.sessionId}.input.sock`),
				onError: reportError,
			});
		} catch (error) {
			reportError(error);
			inputReaderRequested = false;
		}
	}
	const publishTransferMarker = async (includeManaged: boolean): Promise<LiveTransferMarker | undefined> => {
		if (!markerOptions) return undefined;
		const capabilities = [
			...(attachmentConsumer ? ["attachment-lease-v1" as const] : []),
			...(includeManaged ? ["artifact-publish-v1" as const] : []),
			...(inputImageReader ? ["input-image-read-v1" as const] : []),
		];
		if (capabilities.length === 0) return undefined;
		return initializeLiveTransferMarker(
			markerOptions,
			transferTarget,
			capabilities,
			inputImageReader?.descriptor,
		);
	};
	try {
		transferMarker = await publishTransferMarker(managedRequested);
		inputImageReader?.setTransferMarker(transferMarker);
	} catch (error) {
		reportError(error);
		if (inputImageReader) {
			try { await inputImageReader.close(); } catch (cleanupError) { reportError(cleanupError); }
			inputImageReader = undefined;
		}
		if (attachmentConsumer) {
			try { await attachmentConsumer.close(); } catch (cleanupError) { reportError(cleanupError); }
			attachmentConsumer = undefined;
		}
		managedRequested = false;
	}
	if (managedRequested && options.managedArtifacts && transferMarker) {
		try {
			managedArtifactPublisher = await startManagedArtifactPublisher({
				...options.managedArtifacts,
				host: artifactHost,
				expectedTarget: transferTarget,
				onError: reportError,
			});
		} catch (error) {
			reportError(error);
			try { await transferMarker.close(); } catch (cleanupError) { reportError(cleanupError); }
			transferMarker = undefined;
			inputImageReader?.setTransferMarker(undefined);
			managedRequested = false;
			try {
				transferMarker = await publishTransferMarker(false);
				inputImageReader?.setTransferMarker(transferMarker);
			} catch (markerError) {
				reportError(markerError);
				if (inputImageReader) {
					try { await inputImageReader.close(); } catch (cleanupError) { reportError(cleanupError); }
					inputImageReader = undefined;
				}
				if (attachmentConsumer) {
					try { await attachmentConsumer.close(); } catch (cleanupError) { reportError(cleanupError); }
					attachmentConsumer = undefined;
				}
			}
		}
	}
	const server = net.createServer((socket) => {
		socket.on("error", () => { /* Per-client failures never fail the owning server. */ });
		if (closed || clients.size >= limits.maxClients) {
			try {
				socket.end(encode({
					protocol: LIVE_PROTOCOL,
					type: "response",
					ok: false,
					error: { code: "capacity", message: `live client capacity reached (${limits.maxClients})` },
				}));
			} catch {
				socket.destroy();
			}
			return;
		}
		const client: Client = {
			socket,
			input: Buffer.alloc(0),
			queue: [],
			queueBytes: 0,
			blocked: false,
			closed: false,
			subscribed: false,
		};
		clients.add(client);
		client.idleTimer = setTimeout(() => destroyClient(client, new Error("live client did not subscribe before idle timeout")), limits.idleClientTimeoutMs);
		client.idleTimer.unref?.();
		socket.on("drain", () => {
			client.blocked = false;
			flush(client);
		});
		socket.on("data", (chunk: Buffer) => {
			if (client.closed) return;
			client.input = Buffer.concat([client.input, chunk]);
			if (client.input.length > limits.maxInputBytes && !client.input.includes(0x0a)) {
				endWithError(client, new WireError("invalid_request", "input line exceeds byte limit"));
				return;
			}
			while (!client.closed) {
				const newline = client.input.indexOf(0x0a);
				if (newline === -1) break;
				if (newline > limits.maxInputBytes) {
					endWithError(client, new WireError("invalid_request", "input line exceeds byte limit"));
					return;
				}
				const line = client.input.subarray(0, newline);
				client.input = client.input.subarray(newline + 1);
				if (line.length === 0) continue;
				let value: unknown;
				try {
					value = JSON.parse(line.toString("utf8"));
				} catch (error) {
					responseError(client, undefined, new WireError("invalid_request", `invalid JSON: ${error instanceof Error ? error.message : String(error)}`));
					continue;
				}
				handleRequest(client, value);
			}
			if (!client.closed && client.input.length > limits.maxInputBytes) {
				endWithError(client, new WireError("invalid_request", "input line exceeds byte limit"));
			}
		});
		socket.on("close", () => destroyClient(client));
		socket.on("error", () => destroyClient(client));
	});

	try {
		// Socket image readiness is finalized here: after every consumer, the input-byte
		// listener and the final transfer marker have settled, and before `.live.sock`
		// listens. It is computed only from this startup's own snapshot target, consumers
		// and marker. There is no coordinated-activation boolean, environment override,
		// hidden request field, hostname rule, development mode, Sesh-schema condition,
		// start-option switch or function-length probe, and one activation controls both
		// `attachments.imageInput` and strict dispatch. Anything thrown here takes the
		// same startup rollback as a listen failure.
		if (
			attachmentConsumer?.strictImagePreparation === true
			&& inputImageCapability !== undefined
			&& inputImageReader !== undefined
			&& transferMarker !== undefined
			&& transferMarker.capabilities.includes("attachment-lease-v1")
			&& transferMarker.capabilities.includes("input-image-read-v1")
			&& transferMarker.inputImageReader?.listener_generation === inputImageReader.descriptor.listener_generation
		) {
			let finalMarkerOwned = false;
			try {
				// Re-reads the published `consumer.json`, so a replaced or rolled-back
				// marker cannot leave readiness true.
				finalMarkerOwned = await transferMarker.matchesInputImageReader(inputImageReader.descriptor);
			} catch (error) {
				reportError(error);
			}
			if (finalMarkerOwned) attachmentConsumer.activateImageInput();
		}
		await new Promise<void>((resolve, reject) => {
			const onError = (error: Error) => {
				server.off("listening", onListening);
				reject(error);
			};
			const onListening = () => {
				server.off("error", onError);
				resolve();
			};
			server.once("error", onError);
			server.once("listening", onListening);
			server.listen(options.socketPath);
		});
		const stat = await fs.promises.lstat(options.socketPath);
		ownedSocket = { dev: stat.dev, ino: stat.ino };
		await fs.promises.chmod(options.socketPath, 0o600);
	} catch (error) {
		closed = true;
		for (const client of [...clients]) destroyClient(client);
		if (server.listening) await new Promise<void>((resolve) => server.close(() => resolve()));
		if (ownedSocket) {
			try {
				const current = await fs.promises.lstat(options.socketPath);
				if (current.dev === ownedSocket.dev && current.ino === ownedSocket.ino) await fs.promises.unlink(options.socketPath);
			} catch (cleanupError) {
				if ((cleanupError as NodeJS.ErrnoException).code !== "ENOENT") reportError(cleanupError);
			}
		}
		if (managedArtifactPublisher) {
			try { await managedArtifactPublisher.close(); } catch (cleanupError) { reportError(cleanupError); }
		}
		if (inputImageReader) {
			try { await inputImageReader.close(); } catch (cleanupError) { reportError(cleanupError); }
		}
		if (attachmentConsumer) {
			try { await attachmentConsumer.close(); } catch (cleanupError) { reportError(cleanupError); }
		}
		if (transferMarker) {
			try { await transferMarker.close(); } catch (cleanupError) { reportError(cleanupError); }
		}
		detachHost();
		throw error;
	}
	server.on("error", reportError);

	const close = async (): Promise<void> => {
		if (closing) return closing;
		closing = (async () => {
			closed = true;
			if (managedArtifactPublisher) {
				try { await managedArtifactPublisher.close(); } catch (error) { reportError(error); }
			}
			if (inputImageReader) {
				try { await inputImageReader.close(); } catch (error) { reportError(error); }
			}
			detachHost();
			for (const client of [...clients]) destroyClient(client);
			await new Promise<void>((resolve) => server.close(() => resolve()));
			if (attachmentConsumer) {
				try { await attachmentConsumer.close(); } catch (error) { reportError(error); }
			}
			if (transferMarker) {
				try { await transferMarker.close(); } catch (error) { reportError(error); }
			}
			try {
				const current = await fs.promises.lstat(options.socketPath);
				if (ownedSocket && current.dev === ownedSocket.dev && current.ino === ownedSocket.ino) {
					await fs.promises.unlink(options.socketPath);
				}
			} catch (error) {
				if ((error as NodeJS.ErrnoException).code !== "ENOENT") reportError(error);
			}
		})();
		return closing;
	};

	return { socketPath: options.socketPath, close };
}

/** Start the canonical `<sessionId>.live.sock` listener for an owning live session. */
export function startLiveSessionTransport(
	host: LiveTransportHost,
	sessionId: string,
	options: StartLiveSessionTransportOptions = {},
): Promise<LiveTransportServer> {
	const { socketsDir, ...transportOptions } = options;
	return startLiveTransport(host, {
		...transportOptions,
		socketPath: liveSocketPath(sessionId, socketsDir),
		inputImageSocketPath: inputImageSocketPath(sessionId, socketsDir),
	});
}
