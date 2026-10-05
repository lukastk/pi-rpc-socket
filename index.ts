/**
 * RPC Socket Extension for Pi
 *
 * Opens a Unix socket server inside the interactive TUI session so external
 * processes can inject messages into the live conversation and receive
 * streaming responses.
 *
 * Each session gets its own socket at <tmpdir>/pi-rpc-sockets/<sessionId>.sock,
 * where <tmpdir> is $TMPDIR if set (e.g. on Termux where /tmp doesn't exist)
 * and /tmp otherwise.
 *
 * Protocol (JSONL, one object per line):
 *
 *   Commands:
 *     {"message":"prompt text"}           Send a message (delivered as steer)
 *     {"subscribe":true}                  Start receiving Pi events
 *     {"abort":true}                      Cancel current agent operation
 *     {"compact":true}                    Compact; ACK only on completion/error
 *     {"id":"...","capabilities":true}    Discover protocol 2 and command resources
 *     {"id":"...","command":"/..."}       Explicit command (returns an operation)
 *     {"id":"...","getOperation":"..."}   Poll a command, never re-execute it
 *     {"getState":true}                   Query agent state (idle, context, cwd, tmux)
 *     {"getTmuxInfo":true}                Query tmux session/pane info
 *     {"appendSystemPrompt":"..."}        Append to system prompt (persistent)
 *     {"clearSystemPrompt":true}          Remove appended system prompt
 *     {"set_model":"<pattern>"}           Switch the conversation model live
 *
 *   Responses:
 *     {"ok":true,...}                     Success
 *     {"error":"reason"}                  Error
 *
 *   Streamed events (subscribers only, only for socket-initiated turns):
 *     {"event":"text_delta","delta":"Hello "}
 *     {"event":"tool_execution_start","toolName":"web_search"}
 *     {"event":"tool_execution_end","toolName":"web_search"}
 *     {"event":"agent_end","stopReason":"stop","model":"...","provider":"...","usage":{...}}
 */
import * as net from "node:net";
import * as fs from "node:fs";
import * as path from "node:path";
import { execFileSync } from "node:child_process";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import type { AssistantMessage } from "@earendil-works/pi-ai";
import { Commands } from "./commands.ts";

/**
 * Pick a sockets-dir whose Unix-domain socket paths will fit inside
 * sun_path. macOS caps that struct at 104 bytes (Linux at 108); a Pi
 * session ID is a 36-char UUID, plus "/" + ".sock" = 42 chars, so we
 * leave ~50 bytes of headroom. The default macOS $TMPDIR is
 * /var/folders/<u>/<n>/T/ (≈47 chars) which alone would make the full
 * socket path overflow. Try $TMPDIR first (matches the original
 * intent and Termux/Linux behavior); fall back to /tmp, then /var/tmp.
 */
function pickSocketsDir(): string {
	const SUN_PATH_LIMIT = 104;
	const RESERVED_FOR_SOCKETNAME = 50;
	const seen = new Set<string>();
	const candidates: string[] = [];
	for (const c of [process.env.TMPDIR, "/tmp", "/var/tmp"]) {
		if (!c) continue;
		const trimmed = c.replace(/\/+$/, "");
		if (seen.has(trimmed)) continue;
		seen.add(trimmed);
		candidates.push(trimmed);
	}
	for (const c of candidates) {
		const dir = path.join(c, "pi-rpc-sockets");
		if (dir.length + RESERVED_FOR_SOCKETNAME <= SUN_PATH_LIMIT) return dir;
	}
	// No candidate fits; return the first one anyway and let listen() error
	// loudly rather than silently picking something surprising.
	return path.join(candidates[0] ?? "/tmp", "pi-rpc-sockets");
}

const SOCKETS_DIR = pickSocketsDir();

/**
 * Resolve a model pattern to a single Model, mirroring how Pi's own model
 * resolution works for `--model` / `--models` patterns and the TUI model
 * picker. Pi's resolver lives in `core/model-resolver.ts`
 * (`findExactModelReferenceMatch` + the partial-match fallback used by
 * `resolveModelScope`/`tryMatchModel`), but those functions are not part of
 * the package's public export surface (only "." is exported), so we cannot
 * import them from an extension. This is a faithful port of that logic over
 * the public `ModelRegistry` API.
 *
 * Matching precedence (same as Pi):
 *   1. Exact canonical reference `provider/id` (case-insensitive).
 *   2. Exact bare `id` (rejected if ambiguous across providers).
 *   3. Partial substring match on `id`/`name`, preferring an alias
 *      (e.g. `claude-sonnet-4-5`) over dated versions, then highest-sorting.
 *
 * Returns the resolved Model, or undefined if the pattern matches nothing.
 */
function isModelAlias(id: string): boolean {
	if (id.endsWith("-latest")) return true;
	return !/-\d{8}$/.test(id);
}

function resolveModelByPattern(pattern: string, models: any[]): any | undefined {
	const trimmed = pattern.trim();
	if (!trimmed) return undefined;
	const normalized = trimmed.toLowerCase();

	// 1. Exact canonical "provider/id"
	const canonical = models.filter(
		(m) => `${m.provider}/${m.id}`.toLowerCase() === normalized,
	);
	if (canonical.length === 1) return canonical[0];
	if (canonical.length > 1) return undefined;

	// 2. "provider/modelId" split (when the slash is a provider separator)
	const slash = trimmed.indexOf("/");
	if (slash !== -1) {
		const provider = trimmed.slice(0, slash).trim().toLowerCase();
		const modelId = trimmed.slice(slash + 1).trim().toLowerCase();
		if (provider && modelId) {
			const byProvider = models.filter(
				(m) => m.provider.toLowerCase() === provider && m.id.toLowerCase() === modelId,
			);
			if (byProvider.length === 1) return byProvider[0];
			if (byProvider.length > 1) return undefined;
		}
	}

	// 3. Exact bare id
	const byId = models.filter((m) => m.id.toLowerCase() === normalized);
	if (byId.length === 1) return byId[0];
	if (byId.length > 1) return undefined;

	// 4. Partial substring match on id/name, alias-preferred
	const partial = models.filter(
		(m) =>
			m.id.toLowerCase().includes(normalized) ||
			(typeof m.name === "string" && m.name.toLowerCase().includes(normalized)),
	);
	if (partial.length === 0) return undefined;
	const aliases = partial.filter((m) => isModelAlias(m.id));
	const pool = aliases.length > 0 ? aliases : partial;
	pool.sort((a, b) => b.id.localeCompare(a.id));
	return pool[0];
}

/**
 * Detect tmux session/pane info from the Pi process's environment.
 * Returns null if Pi was launched outside tmux.
 */
function getTmuxInfo(): Record<string, unknown> | null {
	const tmuxEnv = process.env.TMUX;
	const paneId = process.env.TMUX_PANE;
	if (!tmuxEnv || !paneId) return null;

	// Static info from env
	const info: Record<string, unknown> = {
		inTmux: true,
		paneId,
		socketPath: tmuxEnv.split(",")[0],
	};

	// Query tmux for human-readable session/window/pane names.
	// These can change as the user renames things, so query fresh each time.
	try {
		const output = execFileSync(
			"tmux",
			["display-message", "-p", "-t", paneId, "#S\t#W\t#I\t#P"],
			{ encoding: "utf8", timeout: 1000 },
		).trim();
		const [session, window, windowIndex, paneIndex] = output.split("\t");
		info.session = session;
		info.window = window;
		info.windowIndex = parseInt(windowIndex, 10);
		info.paneIndex = parseInt(paneIndex, 10);
	} catch {
		// tmux not available or query failed — return basic info only
	}

	return info;
}

export default function (pi: ExtensionAPI) {
	let server: net.Server | null = null;
	let socketPath: string | null = null;
	const subscribers = new Set<net.Socket>();

	// --- System prompt injection ---
	// Accumulated text appended to the system prompt via before_agent_start.
	let appendedSystemPrompt: string | null = null;

	pi.on("before_agent_start", async (event) => {
		if (!appendedSystemPrompt) return;
		return {
			systemPrompt: (event as any).systemPrompt + "\n\n" + appendedSystemPrompt,
		};
	});

	// --- Event attribution ---
	// Track whether the current agent turn was initiated via the socket.
	// Only broadcast events to subscribers for socket-initiated turns,
	// so typing in the TUI doesn't trigger voice output.
	let currentTurnFromSocket = false;
	let pendingSocketMessage = false;

	pi.on("agent_start", async () => {
		currentTurnFromSocket = pendingSocketMessage;
		pendingSocketMessage = false;
	});

	// --- Latest ctx reference for abort/compact/state ---
	// Event handlers receive ctx; we capture the latest one so socket
	// commands can call ctx.abort() etc. outside of an event handler.
	let latestCtx: ExtensionContext | null = null;
	const context = () => {
		if (!latestCtx) throw new Error("no session context available");
		return latestCtx;
	};
	const commands = new Commands(pi, context, setModel, () => { pendingSocketMessage = true; });
	async function setModel(pattern: string) {
		if (!pattern.trim()) throw new Error("set_model: empty pattern");
		const ctx = context();
		ctx.modelRegistry.refresh();
		const model = resolveModelByPattern(pattern, ctx.modelRegistry.getAvailable());
		if (!model) throw new Error(`set_model: unknown model "${pattern}" (no available model matches)`);
		if (!await pi.setModel(model)) throw new Error(`set_model: no configured auth for ${model.provider}/${model.id}`);
		return { model: { provider: model.provider, id: model.id } };
	}

	// Session ID captured at session_start (the socket filename is derived from it).
	// Stable for the lifetime of this session; avoids relying on latestCtx for it.
	let sessionId: string | null = null;

	// Capture ctx from any event that provides it
	const captureCtx = async (_event: any, ctx: any) => { latestCtx = ctx; };
	pi.on("session_start", captureCtx);
	pi.on("agent_start", captureCtx);
	pi.on("agent_end", captureCtx);

	// --- Broadcast to subscribers ---
	function broadcast(data: Record<string, unknown>) {
		if (!currentTurnFromSocket) return;
		const line = JSON.stringify(data) + "\n";
		for (const conn of subscribers) {
			try {
				conn.write(line);
			} catch {
				subscribers.delete(conn);
			}
		}
	}

	// --- Forward Pi events to subscribers ---
	pi.on("message_update", async (event) => {
		const evt = (event as any).assistantMessageEvent;
		if (evt?.type === "text_delta") {
			broadcast({ event: "text_delta", delta: evt.delta });
		}
	});

	pi.on("tool_execution_start", async (event) => {
		broadcast({
			event: "tool_execution_start",
			toolName: (event as any).toolName,
		});
	});

	pi.on("tool_execution_end", async (event) => {
		broadcast({
			event: "tool_execution_end",
			toolName: (event as any).toolName,
		});
	});

	pi.on("agent_end", async (event) => {
		// Pluck the final AssistantMessage from the turn so subscribers receive
		// token usage / cost / stopReason without having to re-read the on-disk
		// JSONL. Matches the shape produced by `pi -p --mode json`'s agent_end.
		const messages = (event as any).messages as unknown[] | undefined;
		let last: AssistantMessage | undefined;
		if (Array.isArray(messages)) {
			for (let i = messages.length - 1; i >= 0; i--) {
				const m = messages[i] as { role?: string };
				if (m && m.role === "assistant") {
					last = m as AssistantMessage;
					break;
				}
			}
		}
		const payload: Record<string, unknown> = { event: "agent_end" };
		if (last) {
			payload.stopReason = last.stopReason;
			payload.model = last.model;
			payload.provider = last.provider;
			payload.usage = last.usage;
		}
		broadcast(payload);
		currentTurnFromSocket = false;
	});

	// --- Socket server ---
	pi.on("session_start", async (_event, ctx) => {
		latestCtx = ctx;
		sessionId = ctx.sessionManager.getSessionId();

		fs.mkdirSync(SOCKETS_DIR, { recursive: true });

		socketPath = path.join(SOCKETS_DIR, `${sessionId}.sock`);

		try {
			fs.unlinkSync(socketPath);
		} catch {
			// doesn't exist, fine
		}

		server = net.createServer((conn) => {
			let buffer = "";
			conn.setEncoding("utf8"); // Preserve multibyte characters split across network reads.

			conn.on("data", (data) => {
				buffer += data;
				const lines = buffer.split("\n");
				buffer = lines.pop()!;

				for (const line of lines) {
					const trimmed = line.trim();
					if (!trimmed) continue;

					let parsed: any;
					try {
						parsed = JSON.parse(trimmed);
					} catch (e: any) {
						conn.write(JSON.stringify({ error: `invalid JSON: ${e.message}` }) + "\n");
						continue;
					}

					try {
						handleCommand(parsed, conn);
					} catch (e: any) {
						conn.write(JSON.stringify({ id: parsed?.id, ok: false, error: `command failed: ${e.message}` }) + "\n");
					}
				}
			});

			conn.on("close", () => {
				subscribers.delete(conn);
			});

			conn.on("error", () => {
				subscribers.delete(conn);
			});
		});

		server.listen(socketPath, () => {
			if (ctx.hasUI) {
				ctx.ui.notify(`RPC socket: ${socketPath}`, "info");
			}
		});

		server.on("error", (err) => {
			if (ctx.hasUI) {
				ctx.ui.notify(`RPC socket error: ${err.message}`, "error");
			}
		});
	});

	function handleCommand(parsed: any, conn: net.Socket) {
		if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) throw new Error("request must be an object");
		if (parsed.id !== undefined && (typeof parsed.id !== "string" || !/^[A-Za-z0-9_-]{1,128}$/.test(parsed.id))) throw new Error("invalid request id");
		const operations = ["message", "subscribe", "abort", "compact", "getState", "getTmuxInfo", "appendSystemPrompt", "clearSystemPrompt", "set_model", "capabilities", "command", "getOperation"];
		if (operations.filter(k => Object.hasOwn(parsed, k)).length !== 1) throw new Error("request must contain exactly one operation");
		const reply = (data: Record<string, unknown>) => {
			if (!conn.destroyed) conn.write(JSON.stringify({ ...data, id: parsed.id }) + "\n");
		};
		if (parsed.capabilities === true) {
			reply({ ok: true, ...commands.capabilities(), sessionId, tmux: getTmuxInfo() });
			return;
		}
		if (typeof parsed.command === "string") {
			if (typeof parsed.id !== "string") throw new Error("command requires a request id");
			reply({ ok: true, operation: commands.start(parsed.id, parsed.command) });
			return;
		}
		if (typeof parsed.getOperation === "string") {
			reply({ ok: true, operation: commands.get(parsed.getOperation) });
			return;
		}

		// Subscribe to events
		if (parsed.subscribe === true) {
			subscribers.add(conn);
			reply({ ok: true, subscribed: true });
			return;
		}

		// Abort current operation
		if (parsed.abort === true) {
			if (!latestCtx) {
				reply({ error: "no context available" });
				return;
			}
			try {
				latestCtx.abort();
				reply({ ok: true, aborted: true });
			} catch (e: any) {
				reply({ error: `abort failed: ${e.message}` });
			}
			return;
		}

		// Legacy direct operation: reply ONLY on real completion, never at kickoff.
		// The v2 command operation supports polling without holding a connection open.
		if (parsed.compact === true) {
			if (parsed.instructions !== undefined && typeof parsed.instructions !== "string") throw new Error("instructions must be text");
			void commands.compact(parsed.instructions).then(
				result => reply({ ok: true, compacted: true, result }),
				error => reply({ ok: false, error: error.message }),
			);
			return;
		}

		// Query state
		if (parsed.getState === true) {
			if (!latestCtx) {
				reply({ error: "no context available" });
				return;
			}
			const idle = latestCtx.isIdle();
			const usage = latestCtx.getContextUsage?.() ?? null;
			// Current model/provider come from ctx.model (may be undefined if no
			// model is configured). Pi exposes a single thinking level via
			// pi.getThinkingLevel(); providers map it to their own representation
			// (OpenAI "reasoning_effort", Anthropic budget tokens, etc.). There
			// is no separate top-level "effort" field on the SDK.
			const model = latestCtx.model;
			const config = {
				provider: model?.provider ?? null,
				model: model?.id ?? null,
				thinkingLevel: pi.getThinkingLevel(),
			};
			reply({
				ok: true,
				state: {
					sessionId,
					idle,
					contextUsage: usage,
					hasAppendedSystemPrompt: appendedSystemPrompt !== null,
					cwd: process.cwd(),
					tmux: getTmuxInfo() ?? { inTmux: false },
					config,
				},
			});
			return;
		}

		// Query tmux info specifically
		if (parsed.getTmuxInfo === true) {
			reply({
				ok: true,
				tmux: getTmuxInfo() ?? { inTmux: false },
			});
			return;
		}

		// Append to system prompt (persistent across turns)
		if (typeof parsed.appendSystemPrompt === "string") {
			appendedSystemPrompt = parsed.appendSystemPrompt;
			reply({ ok: true, appendedSystemPrompt: true });
			return;
		}

		if (typeof parsed.set_model === "string") {
			void setModel(parsed.set_model).then(result => reply({ ok: true, ...result }),
				error => reply({ ok: false, error: error.message }));
			return;
		}

		// Clear appended system prompt
		if (parsed.clearSystemPrompt === true) {
			appendedSystemPrompt = null;
			reply({ ok: true, clearedSystemPrompt: true });
			return;
		}

		// Send message
		if (typeof parsed.message === "string" && parsed.message.trim()) {
			pendingSocketMessage = true;
			pi.sendUserMessage(parsed.message, { deliverAs: "steer", expandPromptTemplates: false });
			reply({ ok: true, delivered: parsed.message, status: "submitted" });
			return;
		}

		reply({ error: "unknown command" });
	}

	pi.on("session_shutdown", async (event) => {
		// Notify subscribers before tearing down so clients can distinguish a
		// clean shutdown from an unexpected ECONNRESET. Best-effort: ignore
		// per-conn write failures.
		const reason = (event as any)?.reason;
		const shutdownLine =
			JSON.stringify({ event: "session_shutdown", reason: reason ?? null }) + "\n";
		for (const conn of subscribers) {
			try { conn.write(shutdownLine); } catch {}
		}
		subscribers.clear();
		if (server) {
			server.close();
			server = null;
		}
		if (socketPath) {
			try {
				fs.unlinkSync(socketPath);
			} catch {
				// already gone
			}
			socketPath = null;
		}
	});
}
