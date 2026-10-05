import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";

export type Operation = {
	status: "running" | "completed" | "submitted" | "failed";
	result?: unknown;
	error?: string;
};
type Entry = { operation: Operation; finishedAt?: number };
export const commandNames = ["compact", "model", "thinking", "name", "session"];
const deferred = new Set(["new", "resume", "tree", "fork", "clone", "import", "reload", "quit",
	"settings", "scoped-models", "login", "logout", "trust", "copy", "export", "share", "bug", "llama", "hotkeys", "changelog"]);
const levels = ["off", "minimal", "low", "medium", "high", "xhigh"] as const;

// Uses only Pi's public extension API. Its void sendUserMessage cannot report
// resource-handler completion: those operations are honestly marked submitted.
export class Commands {
	private entries = new Map<string, Entry>();
	private compactInFlight = false;
	constructor(
		private pi: ExtensionAPI,
		private context: () => ExtensionContext,
		private setModel: (pattern: string) => Promise<unknown>,
		private beforeMessage: () => void,
		private now: () => number = Date.now,
	) {}

	capabilities() {
		return {
			protocol: 2,
			commands: commandNames,
			resources: this.pi.getCommands(),
			message: "steer",
			resourceOutcome: "submitted",
			operationRetentionSeconds: 3600,
		};
	}

	private prune() {
		for (const [id, entry] of this.entries) {
			if (entry.finishedAt !== undefined && this.now() - entry.finishedAt >= 3600_000) this.entries.delete(id);
		}
	}

	get(id: string): Operation {
		this.prune();
		const entry = this.entries.get(id);
		if (!entry) throw new Error("unknown or expired operation; it may have been lost on reload/exit — do not automatically retry");
		return entry.operation;
	}

	start(id: string, text: string): Operation {
		if (!/^[A-Za-z0-9_-]{1,128}$/.test(id)) throw new Error("command requires a request id (1–128 letters, digits, _ or -)");
		const match = /^\/([^\s]+)(?:\s+([\s\S]*))?$/.exec(text.trim());
		if (!match) throw new Error("command must start with / followed by its name");
		const [, name, rest] = match;
		const args = rest === undefined ? "" : rest;
		const resource = this.pi.getCommands().find(c => c.name === name);
		if (deferred.has(name) || (!commandNames.includes(name) && !resource)) throw new Error(`unsupported RPC command /${name}; use Pi's terminal explicitly`);
		this.prune();
		if (this.entries.has(id)) throw new Error("duplicate request id; poll the existing operation instead of resending");
		if (this.entries.size >= 256) throw new Error("RPC operation capacity reached (256); completed entries expire after 1 hour");
		const entry: Entry = { operation: { status: "running" } };
		this.entries.set(id, entry);
		void this.execute(name, args, text).then(operation => {
			entry.operation = operation;
			entry.finishedAt = this.now();
		}, error => {
			entry.operation = { status: "failed", error: error instanceof Error ? error.message : String(error) };
			entry.finishedAt = this.now();
		});
		return entry.operation;
	}

	async compact(instructions?: string): Promise<unknown> {
		const ctx = this.context();
		if (this.compactInFlight || !ctx.isIdle() || ctx.hasPendingMessages()) throw new Error("compact requires an idle session with no queued messages or running compaction");
		this.compactInFlight = true;
		try {
			return await new Promise((resolve, reject) => ctx.compact({
				customInstructions: instructions, onComplete: resolve, onError: reject,
			}));
		} finally { this.compactInFlight = false; }
	}

	private async execute(name: string, args: string, text: string): Promise<Operation> {
		const ctx = this.context();
		let result: unknown;
		switch (name) {
			case "compact":
				result = await this.compact(args || undefined);
				break;
			case "model":
				if (!args.trim()) throw new Error("/model requires a model argument over RPC (no interactive picker)");
				result = await this.setModel(args.trim());
				break;
			case "thinking":
				if (args) {
					if (!(levels as readonly string[]).includes(args)) throw new Error(`invalid thinking level: ${args}`);
					this.pi.setThinkingLevel(args as typeof levels[number]);
				}
				result = { thinkingLevel: this.pi.getThinkingLevel() }; // Pi may clamp to model capabilities.
				break;
			case "name":
				if (args) this.pi.setSessionName(args);
				result = { name: this.pi.getSessionName() ?? null };
				break;
			case "session":
				if (args) throw new Error("/session takes no arguments");
				result = {
					sessionId: ctx.sessionManager.getSessionId(), name: this.pi.getSessionName() ?? null,
					cwd: ctx.cwd, idle: ctx.isIdle(), pendingMessages: ctx.hasPendingMessages(),
					contextUsage: ctx.getContextUsage() ?? null,
					model: ctx.model ? { provider: ctx.model.provider, id: ctx.model.id } : null,
					thinkingLevel: this.pi.getThinkingLevel(),
				};
				break;
			default:
				// Extension handlers may never start a turn. Arming the stream for
				// them would misattribute the NEXT human-typed turn to this request.
				if (this.pi.getCommands().find(c => c.name === name)?.source !== "extension") this.beforeMessage();
				this.pi.sendUserMessage(text.trim(), { deliverAs: "steer", expandPromptTemplates: true });
				return { status: "submitted", result: { note: "submitted to Pi's resource dispatcher; async completion/errors are reported by Pi, not this acknowledgement" } };
		}
		return { status: "completed", result };
	}
}
