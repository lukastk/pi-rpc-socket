import * as path from "node:path";

const MAX_SOCKET_PATH_BYTES = 103; // macOS sun_path is 104 bytes including the trailing NUL.
const SOCKETS_DIRECTORY = "pi-rpc-sockets";
const MAX_SOCKET_FILENAME_BYTES = 50;

/** Pick the shared socket directory while reserving room for legacy, live, and input-reader names. */
export function pickSocketsDir(): string {
	const seen = new Set<string>();
	const candidates: string[] = [];
	for (const candidate of [process.env.TMPDIR, "/tmp", "/var/tmp"]) {
		if (!candidate) continue;
		const trimmed = candidate.replace(/\/+$/, "");
		if (seen.has(trimmed)) continue;
		seen.add(trimmed);
		candidates.push(trimmed);
	}
	for (const candidate of candidates) {
		const directory = path.join(candidate, SOCKETS_DIRECTORY);
		if (Buffer.byteLength(directory) + 1 + MAX_SOCKET_FILENAME_BYTES <= MAX_SOCKET_PATH_BYTES) return directory;
	}
	// Let listen() fail loudly if even the shortest documented candidate cannot fit.
	return path.join(candidates[0] ?? "/tmp", SOCKETS_DIRECTORY);
}

export function legacySocketPath(sessionId: string, socketsDir = pickSocketsDir()): string {
	return socketPath(socketsDir, `${validateSessionId(sessionId)}.sock`);
}

export function liveSocketPath(sessionId: string, socketsDir = pickSocketsDir()): string {
	return socketPath(socketsDir, `${validateSessionId(sessionId)}.live.sock`);
}

export function inputImageSocketPath(sessionId: string, socketsDir = pickSocketsDir()): string {
	return socketPath(socketsDir, `${validateSessionId(sessionId)}.input.sock`);
}

function validateSessionId(sessionId: string): string {
	if (!/^[A-Za-z0-9_-]{1,128}$/.test(sessionId)) {
		throw new Error("sessionId must be 1-128 ASCII letters, digits, _ or -");
	}
	return sessionId;
}

function socketPath(directory: string, filename: string): string {
	const result = path.join(directory, filename);
	if (Buffer.byteLength(result) > MAX_SOCKET_PATH_BYTES) {
		throw new Error(`Unix socket path exceeds ${MAX_SOCKET_PATH_BYTES} bytes: ${result}`);
	}
	return result;
}
