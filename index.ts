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
 *     {"compact":true}                    Trigger context compaction
 *     {"getState":true}                   Query agent state (idle, context, cwd, tmux)
 *     {"getTmuxInfo":true}                Query tmux session/pane info
 *     {"appendSystemPrompt":"..."}        Append to system prompt (persistent)
 *     {"clearSystemPrompt":true}          Remove appended system prompt
 *
 *   Responses:
 *     {"ok":true,...}                     Success
 *     {"error":"reason"}                  Error
 *
 *   Streamed events (subscribers only, only for socket-initiated turns):
 *     {"event":"text_delta","delta":"Hello "}
 *     {"event":"tool_execution_start","toolName":"web_search"}
 *     {"event":"tool_execution_end","toolName":"web_search"}
 *     {"event":"agent_end"}
 */
import * as net from "node:net";
import * as fs from "node:fs";
import * as path from "node:path";
import { execFileSync } from "node:child_process";
import type { ExtensionAPI } from "@mariozechner/pi-coding-agent";

const SOCKETS_DIR = path.join(process.env.TMPDIR || "/tmp", "pi-rpc-sockets");

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
	let latestCtx: any = null;

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

	pi.on("agent_end", async () => {
		broadcast({ event: "agent_end" });
		currentTurnFromSocket = false;
	});

	// --- Socket server ---
	pi.on("session_start", async (_event, ctx) => {
		latestCtx = ctx;
		const sessionId = ctx.sessionManager.getSessionId();

		fs.mkdirSync(SOCKETS_DIR, { recursive: true });

		socketPath = path.join(SOCKETS_DIR, `${sessionId}.sock`);

		try {
			fs.unlinkSync(socketPath);
		} catch {
			// doesn't exist, fine
		}

		server = net.createServer((conn) => {
			let buffer = "";

			conn.on("data", (data) => {
				buffer += data.toString();
				const lines = buffer.split("\n");
				buffer = lines.pop()!;

				for (const line of lines) {
					const trimmed = line.trim();
					if (!trimmed) continue;

					try {
						const parsed = JSON.parse(trimmed);
						handleCommand(parsed, conn);
					} catch (e: any) {
						conn.write(JSON.stringify({ error: `invalid JSON: ${e.message}` }) + "\n");
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
		const reply = (data: Record<string, unknown>) =>
			conn.write(JSON.stringify(data) + "\n");

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

		// Compact context
		if (parsed.compact === true) {
			if (!latestCtx) {
				reply({ error: "no context available" });
				return;
			}
			latestCtx.compact().then(() => {
				reply({ ok: true, compacted: true });
			}).catch((e: any) => {
				reply({ error: `compact failed: ${e.message}` });
			});
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
			reply({
				ok: true,
				state: {
					idle,
					contextUsage: usage,
					hasAppendedSystemPrompt: appendedSystemPrompt !== null,
					cwd: process.cwd(),
					tmux: getTmuxInfo() ?? { inTmux: false },
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

		// Clear appended system prompt
		if (parsed.clearSystemPrompt === true) {
			appendedSystemPrompt = null;
			reply({ ok: true, clearedSystemPrompt: true });
			return;
		}

		// Send message
		if (typeof parsed.message === "string" && parsed.message.trim()) {
			pendingSocketMessage = true;
			pi.sendUserMessage(parsed.message, { deliverAs: "steer" });
			reply({ ok: true, delivered: parsed.message });
			return;
		}

		reply({ error: "unknown command" });
	}

	pi.on("session_shutdown", async () => {
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
