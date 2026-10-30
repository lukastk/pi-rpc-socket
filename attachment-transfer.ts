import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { createHash, randomUUID } from "node:crypto";
import type { LiveIdentity } from "./live-transport.ts";

export const ATTACHMENT_CAPABILITY = {
	protocol: "sesh.pi-transfer",
	version: 1,
	mode: "lease",
	imageInput: false,
	commandTypes: ["prompt", "steer", "follow_up"],
	maxAttachments: 8,
	maxFileBytes: 16 * 1024 * 1024,
	maxDispatchBytes: 32 * 1024 * 1024,
	maxTextBytesPerFile: 1024 * 1024,
	maxTextBytesTotal: 2 * 1024 * 1024,
	leaseLifetimeMs: 15 * 60 * 1000,
	requestId: "canonical-uuid",
	leaseId: "canonical-uuid",
} as const;

/**
 * The advertised attachment capability. Every field except `imageInput` is fixed;
 * `imageInput` reports the one startup-owned image-admission state of this consumer.
 */
export type LiveAttachmentCapability = Omit<typeof ATTACHMENT_CAPABILITY, "imageInput"> & { imageInput: boolean };

const TEXT_ONLY_ATTACHMENT_CAPABILITY: LiveAttachmentCapability = Object.freeze({ ...ATTACHMENT_CAPABILITY, imageInput: false });
const IMAGE_INPUT_ATTACHMENT_CAPABILITY: LiveAttachmentCapability = Object.freeze({ ...ATTACHMENT_CAPABILITY, imageInput: true });

const TRANSFER_PROTOCOL = "sesh.pi-transfer";
const TRANSFER_VERSION = 1;
const LEASE_TYPE = "attachment-lease";
const LEASE_METADATA_MAX_BYTES = 64 * 1024;
const COPY_BUFFER_BYTES = 64 * 1024;
const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const DIGEST_PATTERN = /^[0-9a-f]{64}$/;
const PROMPT_COMMAND_TYPES = new Set(["prompt", "steer", "follow_up"]);
const NO_FOLLOW = fs.constants.O_NOFOLLOW ?? 0;

/**
 * The only native `PromptFilePreparationErrorCode` values that may be echoed inside
 * bounded retained prose. Pi owns MIME syntax, byte classification and decoding; the
 * socket names the code and never forwards native exception text, paths or values.
 */
const PROMPT_FILE_PREPARATION_CODES: ReadonlySet<string> = new Set([
	"file_not_found",
	"file_not_regular",
	"file_read_failed",
	"text_file_too_large",
	"text_total_too_large",
	"image_processing_failed",
	"invalid_declared_media_type",
	"unsupported_image_type",
	"image_source_too_large",
	"image_output_too_large",
	"image_dimensions_invalid",
	"image_dimensions_exceeded",
	"animated_image_unsupported",
	"image_decode_failed",
	"image_decode_timeout",
	"image_decoder_busy",
	"image_decoder_unavailable",
]);

export type AttachmentTransferErrorCode =
	| "transfer_unavailable"
	| "lease_not_found"
	| "lease_expired"
	| "lease_mismatch"
	| "lease_corrupt"
	| "attachment_prepare_failed"
	| "image_input_unsupported"
	| "stale_target";

export class AttachmentTransferError extends Error {
	constructor(readonly code: AttachmentTransferErrorCode, message: string) {
		super(message);
	}
}

export interface LiveDispatchAttachment {
	leaseId: string;
}

export interface RpcPromptFileInput {
	path: string;
	displayName: string;
	/**
	 * The immutable Sesh lease `media_type`, forwarded byte-for-byte when the lease
	 * carries one. The socket never trims, lower-cases, parses, canonicalizes or
	 * compares it; absent stays absent and an explicitly empty string stays empty.
	 */
	declaredMediaType?: string;
}

export interface PrepareRpcPromptFilesInput {
	command: Record<string, unknown>;
	files: readonly RpcPromptFileInput[];
	maxTextBytesPerFile: number;
	maxTextBytesTotal: number;
	/** Opt-in strict image preparation; omitted unless the exact native capability matched. */
	imageAdmission?: "strict-v1";
}

export type PrepareRpcPromptFiles = (
	input: PrepareRpcPromptFilesInput,
) => Promise<Record<string, unknown>>;

/**
 * The exact command returned by `prepareRpcPromptFiles(...)`, plus whether this
 * dispatch may carry the socket-created strict image-admission option.
 */
export interface PreparedAttachmentDispatch {
	command: Record<string, unknown>;
	strictImageAdmission: boolean;
}

export interface LiveAttachmentConsumerOptions {
	threadId: string;
	inboxPath: string;
	apiSocketPath: string;
	prepareRpcPromptFiles: PrepareRpcPromptFiles;
	/**
	 * The exact validated `LIVE_RPC_IMAGE_ADMISSION_CAPABILITY` imported from the same
	 * module instance as `prepareRpcPromptFiles`. It is compared field-for-field with
	 * the startup snapshot capability by `startLiveTransport(...)`; it is never a
	 * wire, environment or request input.
	 */
	importedImageAdmissionCapability?: unknown;
	markerOwner?: "consumer" | "external";
	/**
	 * Startup-only: strict native preparation support, meaning the imported capability
	 * equalled the capability in the same startup snapshot. Selecting the strict helper
	 * does not by itself admit images; see `activateImageInput()`.
	 */
	strictImagePreparation?: boolean;
}

export type LiveAttachmentEnvironment =
	| { status: "absent" }
	| { status: "invalid"; message: string }
	| { status: "present"; threadId: string; inboxPath: string; apiSocketPath: string };

export interface LiveAttachmentConsumer {
	readonly capability: LiveAttachmentCapability;
	/** Strict native preparation support; it never implies image admission on its own. */
	readonly strictImagePreparation: boolean;
	/**
	 * Startup-only, one-way: publish `imageInput:true` and permit the strict fourth
	 * dispatch argument. One state controls both, so the advertised capability and
	 * strict dispatch cannot diverge. Throws without strict native preparation support
	 * and on a second call.
	 */
	activateImageInput(): void;
	prepare(
		requestId: string,
		target: LiveIdentity,
		command: Record<string, unknown>,
		attachments: readonly LiveDispatchAttachment[],
	): Promise<PreparedAttachmentDispatch>;
	close(): Promise<void>;
}

type LeaseRecord = {
	protocol: string;
	version: number;
	type: string;
	lease_id: string;
	thread_id: string;
	attachment_id: string;
	target: { stream_id: string; session_id: string; session_epoch: number };
	request_id: string;
	name: string;
	media_type?: string;
	size: number;
	sha256: string;
	payload: string;
	expires_at_unix_ms: number;
};

export type LiveTransferCapability = "attachment-lease-v1" | "artifact-publish-v1" | "input-image-read-v1";

export interface LiveInputImageReaderDescriptor {
	protocol: "pi.input-image-read";
	version: 1;
	listener_generation: string;
	socket_suffix: ".input.sock";
}

type ConsumerRecord = {
	protocol: string;
	version: number;
	thread_id: string;
	target: { stream_id: string; session_id: string; session_epoch: number };
	capabilities: LiveTransferCapability[];
	input_image_reader?: LiveInputImageReaderDescriptor;
};

export interface LiveTransferMarker {
	readonly capabilities: readonly LiveTransferCapability[];
	readonly inputImageReader?: Readonly<LiveInputImageReaderDescriptor>;
	matchesInputImageReader(descriptor: LiveInputImageReaderDescriptor): Promise<boolean>;
	close(): Promise<void>;
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return value !== null && typeof value === "object" && !Array.isArray(value);
}

function exactKeys(value: Record<string, unknown>, required: readonly string[], optional: readonly string[] = []): boolean {
	const allowed = new Set([...required, ...optional]);
	return required.every((key) => Object.hasOwn(value, key)) && Object.keys(value).every((key) => allowed.has(key));
}

export function isCanonicalUuid(value: unknown): value is string {
	return typeof value === "string" && UUID_PATTERN.test(value);
}

function sameIdentity(left: LiveIdentity, right: LiveIdentity): boolean {
	return left.streamId === right.streamId && left.sessionEpoch === right.sessionEpoch && left.sessionId === right.sessionId;
}

function targetRecord(target: LiveIdentity): ConsumerRecord["target"] {
	return { stream_id: target.streamId, session_id: target.sessionId, session_epoch: target.sessionEpoch };
}

function safeTarget(value: unknown): value is LeaseRecord["target"] {
	return isRecord(value) && exactKeys(value, ["stream_id", "session_id", "session_epoch"])
		&& typeof value.stream_id === "string" && value.stream_id.length > 0 && value.stream_id.length <= 128
		&& typeof value.session_id === "string" && value.session_id.length > 0 && value.session_id.length <= 128
		&& Number.isSafeInteger(value.session_epoch) && (value.session_epoch as number) >= 0;
}

function safeName(value: unknown): value is string {
	return typeof value === "string"
		&& value !== "."
		&& value !== ".."
		&& Buffer.byteLength(value) >= 1
		&& Buffer.byteLength(value) <= 255
		&& path.basename(value) === value
		&& !value.includes("/")
		&& !value.includes("\\")
		&& !/[\u0000-\u001f\u007f]/.test(value);
}

function safeMediaType(value: unknown): value is string {
	return typeof value === "string"
		&& Buffer.byteLength(value) <= 127
		&& !/[^\x20-\x7e]/.test(value);
}

function parseLease(value: unknown): LeaseRecord {
	if (!isRecord(value) || !exactKeys(
		value,
		[
			"protocol", "version", "type", "lease_id", "thread_id", "attachment_id", "target", "request_id",
			"name", "size", "sha256", "payload", "expires_at_unix_ms",
		],
		["media_type"],
	)) throw new AttachmentTransferError("lease_corrupt", "Lease metadata has an invalid shape");
	if (
		value.protocol !== TRANSFER_PROTOCOL || value.version !== TRANSFER_VERSION || value.type !== LEASE_TYPE
		|| !isCanonicalUuid(value.lease_id) || !isCanonicalUuid(value.thread_id) || !isCanonicalUuid(value.attachment_id)
		|| !isCanonicalUuid(value.request_id) || !safeTarget(value.target) || !safeName(value.name)
		|| (value.media_type !== undefined && !safeMediaType(value.media_type))
		|| !Number.isSafeInteger(value.size) || (value.size as number) < 0 || (value.size as number) > ATTACHMENT_CAPABILITY.maxFileBytes
		|| typeof value.sha256 !== "string" || !DIGEST_PATTERN.test(value.sha256)
		|| value.payload !== "payload"
		|| !Number.isSafeInteger(value.expires_at_unix_ms) || (value.expires_at_unix_ms as number) <= 0
	) throw new AttachmentTransferError("lease_corrupt", "Lease metadata is invalid");
	return value as LeaseRecord;
}

function parseInputImageReaderDescriptor(value: unknown): LiveInputImageReaderDescriptor {
	if (!isRecord(value) || !exactKeys(value, ["protocol", "version", "listener_generation", "socket_suffix"])
		|| value.protocol !== "pi.input-image-read" || value.version !== 1
		|| !isCanonicalUuid(value.listener_generation) || value.socket_suffix !== ".input.sock") {
		throw new AttachmentTransferError("lease_corrupt", "Transfer input-image reader descriptor is invalid");
	}
	return value as unknown as LiveInputImageReaderDescriptor;
}

function parseConsumer(value: unknown): ConsumerRecord {
	if (!isRecord(value) || !exactKeys(value, ["protocol", "version", "thread_id", "target", "capabilities"], ["input_image_reader"])) {
		throw new AttachmentTransferError("lease_corrupt", "Transfer consumer marker has an invalid shape");
	}
	const capabilities = Array.isArray(value.capabilities) ? value.capabilities : [];
	const ordered = ["attachment-lease-v1", "artifact-publish-v1", "input-image-read-v1"] as const;
	const inputImageReader = value.input_image_reader === undefined ? undefined : parseInputImageReaderDescriptor(value.input_image_reader);
	if (
		value.protocol !== TRANSFER_PROTOCOL || value.version !== TRANSFER_VERSION || !isCanonicalUuid(value.thread_id)
		|| !safeTarget(value.target) || capabilities.length < 1 || capabilities.length > ordered.length
		|| capabilities.some((capability, index) => capability !== ordered.filter((candidate) => capabilities.includes(candidate))[index])
		|| new Set(capabilities).size !== capabilities.length
		|| capabilities.includes("input-image-read-v1") !== (inputImageReader !== undefined)
	) throw new AttachmentTransferError("lease_corrupt", "Transfer consumer marker is invalid");
	return {
		protocol: value.protocol,
		version: value.version,
		thread_id: value.thread_id,
		target: value.target,
		capabilities: capabilities as LiveTransferCapability[],
		...(inputImageReader ? { input_image_reader: inputImageReader } : {}),
	};
}

function ownedByCurrentUser(stat: fs.Stats): boolean {
	return typeof process.getuid !== "function" || stat.uid === process.getuid();
}

async function secureDirectory(
	candidate: string,
	mode: number,
	label: string,
	missingCode: AttachmentTransferErrorCode = "lease_corrupt",
): Promise<string> {
	try {
		const normalized = path.resolve(candidate);
		const real = await fs.promises.realpath(normalized);
		const stat = await fs.promises.lstat(normalized);
		if (real !== normalized || !stat.isDirectory() || stat.isSymbolicLink() || !ownedByCurrentUser(stat) || (stat.mode & 0o777) !== mode) {
			throw new Error("unsafe");
		}
		return normalized;
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code === "ENOENT") {
			throw new AttachmentTransferError(missingCode, `${label} is unavailable`);
		}
		throw new AttachmentTransferError("lease_corrupt", `${label} is not an owner-only real directory`);
	}
}

async function secureFile(pathname: string, maxBytes: number, missingCode: AttachmentTransferErrorCode, label: string): Promise<fs.promises.FileHandle> {
	let handle: fs.promises.FileHandle | undefined;
	try {
		handle = await fs.promises.open(pathname, fs.constants.O_RDONLY | NO_FOLLOW);
		const stat = await handle.stat();
		if (!stat.isFile() || !ownedByCurrentUser(stat) || (stat.mode & 0o777) !== 0o600 || stat.size > maxBytes) {
			throw new Error("unsafe");
		}
		return handle;
	} catch (error) {
		await handle?.close().catch(() => undefined);
		if ((error as NodeJS.ErrnoException).code === "ENOENT") {
			throw new AttachmentTransferError(missingCode, `${label} is unavailable`);
		}
		throw new AttachmentTransferError("lease_corrupt", `${label} is not a bounded owner-only regular file`);
	}
}

async function readLeaseMetadata(pathname: string): Promise<LeaseRecord> {
	const handle = await secureFile(pathname, LEASE_METADATA_MAX_BYTES, "lease_not_found", "Lease metadata");
	try {
		const bytes = await handle.readFile();
		if (bytes.length > LEASE_METADATA_MAX_BYTES) throw new AttachmentTransferError("lease_corrupt", "Lease metadata exceeds its byte limit");
		return parseLease(JSON.parse(bytes.toString("utf8")));
	} catch (error) {
		if (error instanceof AttachmentTransferError) throw error;
		throw new AttachmentTransferError("lease_corrupt", "Lease metadata is not strict JSON");
	} finally {
		await handle.close().catch(() => undefined);
	}
}

async function copyVerifiedPayload(sourcePath: string, destinationPath: string, lease: LeaseRecord): Promise<void> {
	const source = await secureFile(sourcePath, ATTACHMENT_CAPABILITY.maxFileBytes, "lease_not_found", "Lease payload");
	let destination: fs.promises.FileHandle | undefined;
	try {
		const before = await source.stat();
		if (before.size !== lease.size) throw new AttachmentTransferError("lease_corrupt", "Lease payload size does not match metadata");
		destination = await fs.promises.open(destinationPath, fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL, 0o600);
		await destination.chmod(0o600);
		const digest = createHash("sha256");
		const buffer = Buffer.allocUnsafe(COPY_BUFFER_BYTES);
		let offset = 0;
		while (offset < lease.size) {
			const wanted = Math.min(buffer.length, lease.size - offset);
			const { bytesRead } = await source.read(buffer, 0, wanted, offset);
			if (bytesRead === 0) throw new AttachmentTransferError("lease_corrupt", "Lease payload ended before its declared size");
			const chunk = buffer.subarray(0, bytesRead);
			digest.update(chunk);
			let written = 0;
			while (written < chunk.length) {
				const result = await destination.write(chunk, written, chunk.length - written, offset + written);
				if (result.bytesWritten === 0) {
					throw new AttachmentTransferError("lease_corrupt", "Verified attachment snapshot write made no progress");
				}
				written += result.bytesWritten;
			}
			offset += bytesRead;
		}
		const extra = Buffer.allocUnsafe(1);
		if ((await source.read(extra, 0, 1, offset)).bytesRead !== 0) {
			throw new AttachmentTransferError("lease_corrupt", "Lease payload exceeds its declared size");
		}
		const after = await source.stat();
		if (after.size !== before.size || after.ino !== before.ino || after.dev !== before.dev) {
			throw new AttachmentTransferError("lease_corrupt", "Lease payload changed during verification");
		}
		if (digest.digest("hex") !== lease.sha256) throw new AttachmentTransferError("lease_corrupt", "Lease payload digest does not match metadata");
		if ((await destination.stat()).size !== lease.size) {
			throw new AttachmentTransferError("lease_corrupt", "Verified attachment snapshot size does not match metadata");
		}
		await destination.sync();
	} catch (error) {
		if (error instanceof AttachmentTransferError) throw error;
		throw new AttachmentTransferError("lease_corrupt", "Lease payload could not be copied safely");
	} finally {
		await destination?.close().catch(() => undefined);
		await source.close().catch(() => undefined);
	}
}

function markerFor(
	threadId: string,
	target: LiveIdentity,
	capabilities: readonly LiveTransferCapability[],
	inputImageReader?: LiveInputImageReaderDescriptor,
): ConsumerRecord {
	return {
		protocol: TRANSFER_PROTOCOL,
		version: TRANSFER_VERSION,
		thread_id: threadId,
		target: targetRecord(target),
		capabilities: [...capabilities],
		...(inputImageReader ? { input_image_reader: { ...inputImageReader } } : {}),
	};
}

function sameInputImageReader(
	left: LiveInputImageReaderDescriptor | undefined,
	right: LiveInputImageReaderDescriptor | undefined,
): boolean {
	if (!left || !right) return left === right;
	return left.protocol === right.protocol && left.version === right.version
		&& left.listener_generation === right.listener_generation && left.socket_suffix === right.socket_suffix;
}

function sameMarker(left: ConsumerRecord, right: ConsumerRecord): boolean {
	return left.protocol === right.protocol && left.version === right.version && left.thread_id === right.thread_id
		&& left.target.stream_id === right.target.stream_id && left.target.session_id === right.target.session_id
		&& left.target.session_epoch === right.target.session_epoch
		&& left.capabilities.length === right.capabilities.length
		&& left.capabilities.every((value, index) => value === right.capabilities[index])
		&& sameInputImageReader(left.input_image_reader, right.input_image_reader);
}

async function writeMarker(inboxPath: string, marker: ConsumerRecord): Promise<void> {
	const temporary = path.join(inboxPath, `.consumer-${randomUUID()}.tmp`);
	const finalPath = path.join(inboxPath, "consumer.json");
	let handle: fs.promises.FileHandle | undefined;
	try {
		handle = await fs.promises.open(temporary, fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | NO_FOLLOW, 0o600);
		await handle.chmod(0o600);
		await handle.writeFile(Buffer.from(JSON.stringify(marker)));
		await handle.sync();
		await handle.close();
		handle = undefined;
		await fs.promises.rename(temporary, finalPath);
		const directory = await fs.promises.open(inboxPath, fs.constants.O_RDONLY);
		try {
			await directory.sync();
		} finally {
			await directory.close();
		}
	} catch {
		await handle?.close().catch(() => undefined);
		await fs.promises.rm(temporary, { force: true }).catch(() => undefined);
		throw new AttachmentTransferError("transfer_unavailable", "Transfer consumer marker could not be published atomically");
	}
}

async function removeOwnedMarker(inboxPath: string, marker: ConsumerRecord): Promise<void> {
	const markerPath = path.join(inboxPath, "consumer.json");
	try {
		const handle = await secureFile(markerPath, LEASE_METADATA_MAX_BYTES, "lease_not_found", "Transfer consumer marker");
		let current: ConsumerRecord;
		try {
			current = parseConsumer(JSON.parse((await handle.readFile()).toString("utf8")));
		} finally {
			await handle.close().catch(() => undefined);
		}
		if (!sameMarker(current, marker)) return;
		await fs.promises.unlink(markerPath);
		const directory = await fs.promises.open(inboxPath, fs.constants.O_RDONLY);
		try {
			await directory.sync();
		} finally {
			await directory.close();
		}
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code === "ENOENT") return;
		if (error instanceof AttachmentTransferError && error.code === "lease_not_found") return;
		throw new AttachmentTransferError("transfer_unavailable", "Transfer consumer marker cleanup failed");
	}
}

export async function initializeLiveTransferMarker(
	options: Pick<LiveAttachmentConsumerOptions, "threadId" | "inboxPath" | "apiSocketPath">,
	target: LiveIdentity,
	capabilities: readonly LiveTransferCapability[],
	inputImageReader?: LiveInputImageReaderDescriptor,
): Promise<LiveTransferMarker> {
	if (!isCanonicalUuid(options.threadId)) throw new AttachmentTransferError("transfer_unavailable", "Transfer thread identity is invalid");
	if (!path.isAbsolute(options.apiSocketPath) || /[\u0000-\u001f\u007f]/.test(options.apiSocketPath)) {
		throw new AttachmentTransferError("transfer_unavailable", "Transfer API socket identity is invalid");
	}
	const canonicalOrder = ["attachment-lease-v1", "artifact-publish-v1", "input-image-read-v1"] as const;
	if (capabilities.length < 1 || capabilities.length > canonicalOrder.length || new Set(capabilities).size !== capabilities.length) {
		throw new AttachmentTransferError("transfer_unavailable", "Transfer consumer capabilities are invalid");
	}
	const ordered = canonicalOrder.filter((capability) => capabilities.includes(capability));
	if (ordered.length !== capabilities.length || ordered.some((capability, index) => capabilities[index] !== capability)) {
		throw new AttachmentTransferError("transfer_unavailable", "Transfer consumer capabilities are invalid");
	}
	if (capabilities.includes("input-image-read-v1") !== (inputImageReader !== undefined)) {
		throw new AttachmentTransferError("transfer_unavailable", "Transfer input-image reader capability and descriptor must be paired");
	}
	if (inputImageReader) parseInputImageReaderDescriptor(inputImageReader);
	const inboxPath = await secureDirectory(options.inboxPath, 0o700, "Transfer inbox");
	const marker = markerFor(options.threadId, target, ordered, inputImageReader);
	await writeMarker(inboxPath, marker);
	let closed = false;
	return {
		capabilities: ordered,
		...(marker.input_image_reader ? { inputImageReader: { ...marker.input_image_reader } } : {}),
		async matchesInputImageReader(descriptor) {
			if (closed || !sameInputImageReader(marker.input_image_reader, descriptor)) return false;
			const markerPath = path.join(inboxPath, "consumer.json");
			try {
				const handle = await secureFile(markerPath, LEASE_METADATA_MAX_BYTES, "lease_not_found", "Transfer consumer marker");
				try {
					const current = parseConsumer(JSON.parse((await handle.readFile()).toString("utf8")));
					return sameMarker(current, marker) && sameInputImageReader(current.input_image_reader, descriptor);
				} finally {
					await handle.close().catch(() => undefined);
				}
			} catch (error) {
				if ((error as NodeJS.ErrnoException).code === "ENOENT") return false;
				if (error instanceof AttachmentTransferError && error.code === "lease_not_found") return false;
				throw error;
			}
		},
		async close() {
			if (closed) return;
			closed = true;
			await removeOwnedMarker(inboxPath, marker);
		},
	};
}

export function readLiveAttachmentEnvironment(env: NodeJS.ProcessEnv = process.env): LiveAttachmentEnvironment {
	const threadId = env.SESH_PI_TRANSFER_THREAD_ID;
	const inboxPath = env.SESH_PI_TRANSFER_INBOX;
	const apiSocketPath = env.SESH_PI_TRANSFER_API_SOCKET;
	const present = [threadId, inboxPath, apiSocketPath].filter((value) => value !== undefined).length;
	if (present === 0) return { status: "absent" };
	if (present !== 3) return { status: "invalid", message: "Sesh Pi transfer environment must contain the complete identity triple" };
	if (!isCanonicalUuid(threadId)) return { status: "invalid", message: "Sesh Pi transfer thread identity must be a canonical UUID" };
	if (!path.isAbsolute(inboxPath!) || !path.isAbsolute(apiSocketPath!) || /[\u0000-\u001f\u007f]/.test(inboxPath! + apiSocketPath!)) {
		return { status: "invalid", message: "Sesh Pi transfer paths must be absolute text without control characters" };
	}
	return { status: "present", threadId, inboxPath: path.resolve(inboxPath!), apiSocketPath: path.resolve(apiSocketPath!) };
}

export async function initializeLiveAttachmentConsumer(
	options: LiveAttachmentConsumerOptions,
	target: LiveIdentity,
	scratchParent = os.tmpdir(),
	now: () => number = Date.now,
): Promise<LiveAttachmentConsumer> {
	if (!isCanonicalUuid(options.threadId)) throw new AttachmentTransferError("transfer_unavailable", "Transfer thread identity is invalid");
	if (!path.isAbsolute(options.apiSocketPath) || /[\u0000-\u001f\u007f]/.test(options.apiSocketPath)) {
		throw new AttachmentTransferError("transfer_unavailable", "Transfer API socket identity is invalid");
	}
	const inboxPath = await secureDirectory(options.inboxPath, 0o700, "Transfer inbox");
	const leasesPath = await secureDirectory(path.join(inboxPath, "leases"), 0o700, "Transfer lease directory");
	const marker = options.markerOwner === "external"
		? undefined
		: await initializeLiveTransferMarker(options, target, ["attachment-lease-v1"]);
	let closed = false;
	const strictImagePreparation = options.strictImagePreparation === true;
	// One startup-owned state behind both the advertised capability and strict dispatch.
	let imageInputActive = false;

	return {
		get capability() {
			return imageInputActive ? IMAGE_INPUT_ATTACHMENT_CAPABILITY : TEXT_ONLY_ATTACHMENT_CAPABILITY;
		},
		strictImagePreparation,
		activateImageInput() {
			if (!strictImagePreparation) {
				throw new Error("Image input requires exact strict native image-admission support");
			}
			if (imageInputActive) throw new Error("Image input has already been activated for this startup");
			imageInputActive = true;
		},
		async prepare(requestId, requestTarget, command, attachments) {
			if (closed) throw new AttachmentTransferError("transfer_unavailable", "Transfer consumer is closed");
			if (!isCanonicalUuid(requestId)) throw new AttachmentTransferError("lease_mismatch", "Attachment dispatch request ID is not canonical");
			if (!sameIdentity(requestTarget, target)) throw new AttachmentTransferError("stale_target", "Attachment dispatch target is stale");
			if (!PROMPT_COMMAND_TYPES.has(command.type as string)) {
				throw new AttachmentTransferError("lease_mismatch", "Attachments are not valid for this native command type");
			}
			if (attachments.length < 1 || attachments.length > ATTACHMENT_CAPABILITY.maxAttachments) {
				throw new AttachmentTransferError("lease_mismatch", "Attachment count is outside the supported bounds");
			}

			let scratch: string | undefined;
			try {
				await fs.promises.mkdir(scratchParent, { recursive: true, mode: 0o700 });
				scratch = await fs.promises.mkdtemp(path.join(scratchParent, ".pi-rpc-attachments-"));
				await fs.promises.chmod(scratch, 0o700);
				const files: RpcPromptFileInput[] = [];
				let aggregate = 0;
				let earliestExpiry = Number.MAX_SAFE_INTEGER;
				for (let index = 0; index < attachments.length; index++) {
					const leaseId = attachments[index]!.leaseId;
					if (!isCanonicalUuid(leaseId)) throw new AttachmentTransferError("lease_mismatch", "Lease ID is not canonical");
					const leaseDirectory = await secureDirectory(path.join(leasesPath, leaseId), 0o700, "Lease directory", "lease_not_found");
					const lease = await readLeaseMetadata(path.join(leaseDirectory, "lease.json"));
					if (lease.expires_at_unix_ms <= now()) throw new AttachmentTransferError("lease_expired", "Attachment lease has expired");
					if (
						lease.lease_id !== leaseId || lease.thread_id !== options.threadId || lease.request_id !== requestId
						|| lease.target.stream_id !== requestTarget.streamId || lease.target.session_id !== requestTarget.sessionId
						|| lease.target.session_epoch !== requestTarget.sessionEpoch
					) throw new AttachmentTransferError("lease_mismatch", "Attachment lease does not match this thread, request, or target");
					aggregate += lease.size;
					if (aggregate > ATTACHMENT_CAPABILITY.maxDispatchBytes) {
						throw new AttachmentTransferError("lease_mismatch", "Attachment aggregate exceeds the dispatch byte limit");
					}
					earliestExpiry = Math.min(earliestExpiry, lease.expires_at_unix_ms);
					const copyPath = path.join(scratch, String(index));
					await copyVerifiedPayload(path.join(leaseDirectory, "payload"), copyPath, lease);
					files.push({
						path: copyPath,
						displayName: lease.name,
						...(lease.media_type === undefined ? {} : { declaredMediaType: lease.media_type }),
					});
				}
				if (earliestExpiry <= now()) throw new AttachmentTransferError("lease_expired", "Attachment lease expired during verification");
				let prepared: Record<string, unknown>;
				try {
					prepared = await options.prepareRpcPromptFiles({
						command,
						files,
						maxTextBytesPerFile: ATTACHMENT_CAPABILITY.maxTextBytesPerFile,
						maxTextBytesTotal: ATTACHMENT_CAPABILITY.maxTextBytesTotal,
						...(strictImagePreparation ? { imageAdmission: "strict-v1" as const } : {}),
					});
				} catch (error) {
					const code = isRecord(error) && typeof error.code === "string" && PROMPT_FILE_PREPARATION_CODES.has(error.code)
						? ` (${error.code})`
						: "";
					throw new AttachmentTransferError("attachment_prepare_failed", `Native attachment preparation failed${code}`);
				}
				if (earliestExpiry <= now()) throw new AttachmentTransferError("lease_expired", "Attachment lease expired during native preparation");
				const imageBearing = Array.isArray(prepared.images) && prepared.images.length > 0;
				if (imageBearing && !imageInputActive) {
					throw new AttachmentTransferError(
						"image_input_unsupported",
						"Image attachment input requires canonical Pi image references; no prompt was dispatched",
					);
				}
				// Forwarded exactly as returned: no reconstruction, JSON round-trip, image
				// stripping or reordering, command-type rewriting or streaming normalization.
				return { command: prepared, strictImageAdmission: imageBearing && imageInputActive };
			} finally {
				if (scratch) {
					try {
						await fs.promises.rm(scratch, { recursive: true, force: true });
					} catch {
						throw new AttachmentTransferError("attachment_prepare_failed", "Verified attachment scratch cleanup failed");
					}
				}
			}
		},
		async close() {
			if (closed) return;
			closed = true;
			await marker?.close();
		},
	};
}
