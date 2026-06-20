# pi-rpc-socket

Pi extension that opens a Unix socket server inside the interactive TUI session so external processes can inject messages, receive streaming responses, abort operations, and inject system prompts into the live conversation. Each session gets its own socket, so multiple pi sessions can run simultaneously.

## How it works

On `session_start`, the extension creates a Unix socket at `<tmpdir>/pi-rpc-sockets/<sessionId>.sock`. The `<tmpdir>` is picked to keep the full socket path inside the OS `sun_path` limit (104 bytes on macOS, 108 on Linux): the extension tries `$TMPDIR` first, then `/tmp`, then `/var/tmp`, and uses the first one whose `<dir>/pi-rpc-sockets/<uuid>.sock` fits. On macOS this means `$TMPDIR` (which defaults to a long `/var/folders/<u>/<n>/T/`) is bypassed in favor of `/tmp`, so the socket actually binds.

On `session_shutdown`, the socket is cleaned up.

## Protocol

One JSON object per line (LF-delimited):

### Commands

| Command | Format |
|---|---|
| Send message | `{"message":"prompt text"}` |
| Subscribe to events | `{"subscribe":true}` |
| Abort current operation | `{"abort":true}` |
| Compact context | `{"compact":true}` |
| Query state | `{"getState":true}` |
| Query tmux info | `{"getTmuxInfo":true}` |
| Append to system prompt | `{"appendSystemPrompt":"voice mode instructions..."}` |
| Clear appended system prompt | `{"clearSystemPrompt":true}` |
| Set conversation model (live) | `{"set_model":"<pattern>"}` |

### Responses

| Response | Format |
|---|---|
| Success (send) | `{"ok":true,"delivered":"prompt text"}` |
| Success (subscribe) | `{"ok":true,"subscribed":true}` |
| Success (abort) | `{"ok":true,"aborted":true}` |
| Success (compact) | `{"ok":true,"compacted":true}` |
| Success (set_model) | `{"ok":true,"model":{"provider":"anthropic","id":"claude-haiku-4-5"},"note":"model set; takes effect on the next turn"}` |
| State | `{"ok":true,"state":{"idle":true,"contextUsage":...,"hasAppendedSystemPrompt":false,"cwd":"/path/to/workdir","tmux":...,"config":{"provider":"anthropic","model":"claude-opus-4-8","thinkingLevel":"high"}}}` |
| Tmux info | `{"ok":true,"tmux":{"inTmux":true,"session":"main","window":"pi","paneId":"%5"}}` |
| Error | `{"error":"reason"}` |

### Streamed events

Subscribed connections receive Pi events as JSONL. Events are only broadcast for turns initiated via the socket (not for messages typed in the TUI):

| Event | Format |
|---|---|
| Text delta | `{"event":"text_delta","delta":"Hello "}` |
| Tool start | `{"event":"tool_execution_start","toolName":"web_search"}` |
| Tool end | `{"event":"tool_execution_end","toolName":"web_search"}` |
| Agent done | `{"event":"agent_end"}` |

## Usage

### Discover active sessions

```bash
ls $TMPDIR/pi-rpc-sockets/
```

Sockets are cleaned up on normal exit. After a crash or SIGKILL, stale socket files may linger. To check if a socket is still alive:

```bash
echo '' | nc -U $TMPDIR/pi-rpc-sockets/<sessionId>.sock -w 1 2>/dev/null && echo "alive" || echo "stale"
```

### Send a message

```bash
echo '{"message":"Run the tests"}' | nc -U $TMPDIR/pi-rpc-sockets/<sessionId>.sock
```

Or target the first available session:

```bash
echo '{"message":"Run the tests"}' | nc -U $(ls $TMPDIR/pi-rpc-sockets/*.sock | head -1)
```

### Subscribe to events

Keep a connection open to stream Pi's output:

```bash
echo '{"subscribe":true}' | nc -U $TMPDIR/pi-rpc-sockets/<sessionId>.sock -k
```

### Abort current operation

```bash
echo '{"abort":true}' | nc -U $TMPDIR/pi-rpc-sockets/<sessionId>.sock
```

### Inject system prompt (persistent)

Appended text is injected via `before_agent_start` on every turn:

```bash
echo '{"appendSystemPrompt":"Always respond in haiku"}' | nc -U $TMPDIR/pi-rpc-sockets/<sessionId>.sock
```

Clear with:

```bash
echo '{"clearSystemPrompt":true}' | nc -U $TMPDIR/pi-rpc-sockets/<sessionId>.sock
```

### Switch the model mid-session

Switch the conversation's model live, without restarting Pi — the same operation as the TUI's model picker / `Ctrl+P` cycling (`session.setModel` under the hood). The new model takes effect on the **next turn**, and `getState` (`state.config.model`) reflects it immediately so a client can confirm the switch:

```bash
echo '{"set_model":"claude-haiku-4-5"}' | nc -U $TMPDIR/pi-rpc-sockets/<sessionId>.sock
# {"ok":true,"model":{"provider":"anthropic","id":"claude-haiku-4-5"},"note":"model set; takes effect on the next turn"}
```

The pattern is resolved with Pi's own model-resolution rules (as used by `--model` / the `/model` picker): an exact `provider/id` reference, then an exact bare `id` (rejected if ambiguous across providers), then a partial substring match on id/name preferring an alias (e.g. `claude-sonnet-4-5`) over dated versions. Only models with configured auth (the ones Pi could actually switch to) are considered.

An unknown or unauthenticated model is a **loud error**, not a silent no-op:

```bash
echo '{"set_model":"no-such-model"}' | nc -U $TMPDIR/pi-rpc-sockets/<sessionId>.sock
# {"error":"set_model: unknown model \"no-such-model\" (no available model matches)"}
```

Like the TUI's model switch, this also updates Pi's saved default model.

### From Node.js

```typescript
import * as net from "node:net";

const conn = net.createConnection("$TMPDIR/pi-rpc-sockets/<sessionId>.sock", () => {
  // Subscribe to events
  conn.write(JSON.stringify({ subscribe: true }) + "\n");
  // Inject system prompt
  conn.write(JSON.stringify({ appendSystemPrompt: "Use <spoken> tags for voice output." }) + "\n");
  // Send a message
  conn.write(JSON.stringify({ message: "What files changed today?" }) + "\n");
});
conn.on("data", (data) => {
  for (const line of data.toString().split("\n").filter(Boolean)) {
    const event = JSON.parse(line);
    if (event.event === "text_delta") process.stdout.write(event.delta);
    if (event.event === "agent_end") conn.end();
  }
});
```

### From Python

```python
import socket, json

sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
sock.connect("$TMPDIR/pi-rpc-sockets/<sessionId>.sock")
sock.sendall(json.dumps({"message": "List open TODOs"}).encode() + b"\n")
print(sock.recv(4096).decode())
sock.close()
```

## Event attribution

Events are only broadcast to subscribers for agent turns that were initiated via the socket. If the user types a command in the TUI, the response is NOT sent to subscribers. This prevents a voice agent from trying to speak responses to typed commands.

## Delivery semantics

Messages are delivered with `deliverAs: "steer"`:
- If the agent is idle, the message triggers an LLM turn immediately.
- If the agent is mid-stream, the message queues until the current tool calls finish, then gets delivered before the next LLM call.

## Install

Install globally as a Pi extension:

```bash
pi -e git:github.com/lukastk/pi-rpc-socket
```

Or add to a Pi extensions manifest (e.g. `~/mysetup/myagent/external_extensions.txt`):

```
git:github.com/lukastk/pi-rpc-socket
```

For local development:

```bash
git clone git@github.com:lukastk/pi-rpc-socket.git
cd pi-rpc-socket
pi -e .
```
