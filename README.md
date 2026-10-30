# pi-rpc-socket

Pi extension that opens a Unix socket server inside the interactive TUI session so external processes can inject messages, receive streaming responses, abort operations, and inject system prompts into the live conversation. Each session gets its own socket, so multiple pi sessions can run simultaneously.

## How it works

On `session_start`, the extension creates the legacy Unix socket at `<tmpdir>/pi-rpc-sockets/<sessionId>.sock`. When the running Pi also exposes the public `registerInteractiveReady(...)` extension capability, the same normal extension factory automatically attaches `context.liveRpcSession` and creates `<sessionId>.live.sock`. A complete native input-image host plus valid Sesh transfer identity additionally creates the private `<sessionId>.input.sock` byte reader. Older Pi versions simply continue with protocol 2; no private import or alternate launcher is used.

The `<tmpdir>` is picked to keep every documented socket name inside the OS `sun_path` limit (104 bytes on macOS, 108 on Linux): the extension tries `$TMPDIR` first, then `/tmp`, then `/var/tmp`, and uses the first fitting directory. On macOS this normally bypasses the long default `$TMPDIR` in favor of `/tmp`. Pi's native session shutdown/replacement lifecycle closes and removes the applicable sockets before the old extension runtime is invalidated.

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

Subscribed connections receive Pi events as JSONL. Events are broadcast for socket-submitted messages and template/skill turns, not
human-typed turns. Arbitrary extension commands are not tagged as turn initiators:
they may never start a turn, and tagging them would misattribute the next human input.

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

Sockets are cleaned up on normal exit. After a crash or SIGKILL, stale socket files may linger. This command is only a liveness hint; a failed connection can also mean a timeout or permission problem and is not by itself permission to unlink the path:

```bash
echo '' | nc -U $TMPDIR/pi-rpc-sockets/<sessionId>.sock -w 1 2>/dev/null && echo "alive" || echo "unreachable (cause unknown)"
```

### Send a message

```bash
echo '{"message":"Run the tests"}' | nc -U $TMPDIR/pi-rpc-sockets/<sessionId>.sock
```

Choose the **exact session UUID**. Never select the first socket: another live
session may belong to unrelated work. Clients that know a tmux pane should verify
`getTmuxInfo` (`paneId` and `socketPath`) before sending.

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

## Live all-origin transport (`live-v1`)

`live-transport.ts` exports a bounded JSONL transport for Pi's public `LiveRpcSession`-shaped host. It is additive: the legacy `.sock` protocol and its socket-origin-only subscribers remain unchanged. The live server uses `<sessionId>.live.sock`; `liveSocketPath()` applies the same `sun_path` checks as legacy discovery.

An existing `.live.sock` is never unlinked merely because a probe failed or timed out. Startup removes it only after a connect attempt reports `ECONNREFUSED` or `ENOENT`; a successful connection is active, while permission, network-stack and timeout failures stop startup loudly.

The transport observes interactive, extension and RPC origins. It subscribes to the host before taking a snapshot, retains a bounded replay suffix, and sends an explicit `gap` plus a fresh snapshot whenever a requested interval is unavailable. Stream, session-epoch and session IDs come from Pi and are required on writes, so a command observed against an old session is refused instead of retargeted.

The package exports `startLiveTransport(host, options)`. The host seam is structural and is implemented by Pi's `LiveRpcSession`:

```ts
interface LiveHostDispatchOptions {
  imageAdmission?: "strict-v1";
}

interface LiveTransportHost {
  snapshot(): LiveSnapshot;
  subscribe(listener: (event: LiveEvent) => void): () => void;
  dispatch(
    target: LiveIdentity,
    command: unknown,
    output: (response: unknown) => void,
    options?: LiveHostDispatchOptions,
  ): Promise<unknown | undefined>;
  close(): void; // detach the observer, never dispose the owning runtime
}
```

The fourth argument is optional out-of-band state that the socket creates itself. It is passed only for a lease-prepared image submission on a runtime that advertises the exact image-admission capability (see below); every other dispatch calls the host with three arguments. Hosts that accept three parameters remain compatible, and function length is never used as a capability check.

The default extension factory capability-detects Pi's public `registerInteractiveReady(...)` API. On supported interactive runtimes it starts `live-v1` from `context.liveRpcSession`; on older Pi versions, print/JSON/RPC modes, and other runtimes without that capability, it leaves the legacy protocol-2 behavior unchanged. The attachment uses no private imports, monkey-patching, second agent, custom launcher, or public TCP listener.

That decision is reported, not left for a client to infer from a missing socket: the protocol-2 `capabilities` reply carries `liveTransport` as `unsupported`, `supported` or `active` (see [Explicit commands](#explicit-commands--protocol-2-020)).

### Requests and frames

Each request is one UTF-8 JSON object followed by LF:

```json
{"protocol":"live-v1","id":"sub-1","op":"subscribe","after":{"streamId":"...","sequence":123}}
{"protocol":"live-v1","id":"snap-1","op":"snapshot"}
{"protocol":"live-v1","id":"write-1","op":"dispatch","target":{"streamId":"...","sessionEpoch":0,"sessionId":"..."},"command":{"type":"prompt","id":"native-1","message":"hello"}}
{"protocol":"live-v1","id":"poll-1","op":"get_result","target":{"streamId":"...","sessionEpoch":0,"sessionId":"..."},"requestId":"write-1"}
{"protocol":"live-v1","id":"caps-1","op":"capabilities"}
{"protocol":"live-v1","id":"history-1","op":"query","target":{"streamId":"...","sessionEpoch":0,"sessionId":"..."},"command":{"type":"history","cursor":{"leafId":"...","before":200},"maxMessages":100,"maxBytes":524288}}
{"protocol":"live-v1","id":"branch-1","op":"query","target":{"streamId":"...","sessionEpoch":0,"sessionId":"..."},"command":{"type":"branch_history","cursor":{"leafId":"...","before":200},"maxItems":100,"maxBytes":524288}}
```

When Sesh has prepared an attachment lease, the additive dispatch shape is:

```json
{"protocol":"live-v1","id":"<canonical-prepare-request-uuid>","op":"dispatch","target":{"streamId":"...","sessionEpoch":0,"sessionId":"..."},"command":{"type":"prompt","id":"native-1","message":"inspect this"},"attachments":[{"leaseId":"<canonical-lease-uuid>"}]}
```

`attachments` is accepted only on `dispatch` for `prompt`, `steer`, and `follow_up`. It contains 1–8 unique ordered lease references; the outer request ID is the canonical UUID used to prepare the leases. Pi renders bounded text files into ordinary native prompt text, which can therefore appear in normal message events and snapshots. Raw lease payloads, paths and image data produced by attachment preparation stay out of JSONL.

An attachment-bearing dispatch must not also carry `command.images`. The field is refused as `invalid_request` — including an explicitly empty array — synchronously, before the mutation ID is reserved and before any lease is read, because native preparation merges prepared lease images with existing command images and caller-supplied base64 would bypass the lease-only transfer boundary. Ordinary non-attachment `command.images` behavior is unchanged.

A transfer-capable runtime advertises the exact optional capability below. Every field is fixed except `imageInput`, which reports whether lease-derived image submission is actually available on this runtime; the example shows a text-only runtime. Unsupported/older runtimes omit `attachments` and keep ordinary `live-v1` behavior:

```json
{"attachments":{"protocol":"sesh.pi-transfer","version":1,"mode":"lease","imageInput":false,"commandTypes":["prompt","steer","follow_up"],"maxAttachments":8,"maxFileBytes":16777216,"maxDispatchBytes":33554432,"maxTextBytesPerFile":1048576,"maxTextBytesTotal":2097152,"leaseLifetimeMs":900000,"requestId":"canonical-uuid","leaseId":"canonical-uuid"}}
```

Stable attachment failure codes are `transfer_unavailable`, `lease_not_found`, `lease_expired`, `lease_mismatch`, `lease_corrupt`, `attachment_prepare_failed`, `image_input_unsupported`, and `image_admission_rejected`. Each is retained and definitive about Pi's standard-user state: no prompt was accepted, and clients must not retry the same request ID. `attachment_prepare_failed` names an exact native preparation code in parentheses when the runtime reported one, and otherwise carries no detail; it never contains native exception text, paths, display names, declared media types, digests or bytes. Attachment dispatch otherwise uses the ordinary retained mutation ledger: its request ID is reserved before lease preparation, duplicate writes are refused, and disconnects are reconciled with target-scoped `get_result` rather than replayed.

The normal extension factory enables this capability only when all three Sesh values are present: `SESH_PI_TRANSFER_THREAD_ID`, `SESH_PI_TRANSFER_INBOX`, and `SESH_PI_TRANSFER_API_SOCKET`. It dynamically capability-detects Pi's public `prepareRpcPromptFiles(...)`; a missing helper or invalid transfer environment is reported while protocol 2 and ordinary `live-v1` remain available. It never uses `SESH_THREAD_ID` or derives identity from an inbox pathname.

Before advertising transfer support, the runtime publishes one atomically owned `consumer.json` for all initialized consumers. It contains protocol/version, the canonical thread UUID, the exact snake-case live target, and a canonical ordered subset of `attachment-lease-v1`, `artifact-publish-v1`, and `input-image-read-v1`. The input-read capability is paired with an exact `input_image_reader` descriptor containing protocol/version, `.input.sock`, and a fresh canonical listener-generation UUID. Attachment preparation, output publication, and the input reader never race as competing marker writers. Each lease is revalidated against thread, request, target, expiry, count/size bounds, owner-only no-symlink storage, exact payload size and streaming SHA-256. Verified bytes are streamed into an owner-only per-dispatch snapshot before native preparation, then removed. Pi's public helper alone owns text/image detection and text merge semantics.

#### Lease-derived image submission

`imageInput` reports one startup-owned state, so the advertised capability and what the socket actually dispatches cannot disagree. It is `true` only when every one of these holds for the same startup snapshot target:

- the running Pi exports the exact `pi.image-admission` capability below, and the identical record appears on that startup snapshot — both are validated field by field;
- that capability comes from the same dynamically imported module instance as `prepareRpcPromptFiles(...)`;
- the attachment lease consumer initialized;
- the exact `pi.input-images` v1 projection capability plus the direct metadata and bounded copy methods are present on that host;
- the private `.input.sock` input-byte listener started for that target; and
- the final atomically published `consumer.json` carries both `attachment-lease-v1` and `input-image-read-v1`, and its `input_image_reader.listener_generation` still matches the live listener when the state is frozen, just before `.live.sock` accepts its first client.

```json
{"protocol":"pi.image-admission","version":1,"preparation":"rpc-prompt-files-strict-v1","dispatch":"standard-user-strict-v1","requestValidation":"normalized-messages-v1","virtualModels":"stable-profile-v1","binaryInLive":false}
```

Nothing else enables it: not helper presence, function arity, a Pi or package version, a later snapshot, an environment variable, a start option, a socket filename, a hostname, a runtime mode, output-artifact presentation, managed output, or a Sesh schema number. A missing, malformed, obsolete or mismatched capability simply leaves `imageInput:false` with protocol 2, ordinary `live-v1`, text attachments, input-image projection and reads, managed output and output-v2 all unaffected. If the input listener or the final marker fails, image input stays false; if marker publication rolls attachment support back, the `attachments` capability is omitted entirely as before.

When the exact capability matched, preparation runs as `imageAdmission:"strict-v1"` so that Pi — not the socket — classifies bytes and decides MIME syntax. Each verified lease's immutable `media_type` is forwarded byte for byte as the native `declaredMediaType`; absent stays absent, an empty declaration stays empty, and the socket never trims, lower-cases, parses, canonicalizes, compares or sniffs it. A prepared image is dispatched only while `imageInput` is true, and then the exact object Pi returned is passed to `host.dispatch(...)` together with `{ imageAdmission: "strict-v1" }`: no reconstruction, JSON round-trip, image stripping or reordering, command-type rewriting, native-ID substitution or streaming normalization. Text-only prepared commands dispatch with no options at all. While `imageInput` is false, a prepared image-bearing command is still refused with retained `image_input_unsupported` and no prompt is dispatched.

Identity domains stay separate. The lease SHA-256 authenticates the raw uploaded bytes and is verified before preparation; strict native preparation may keep, convert or resize those bytes; Pi mints and owns every input-image identity and digest. The socket never compares the lease digest with a native digest, never reuses a lease, attachment, request, publication or mapping ID as an input-image ID, and keeps no correlation record. `binaryInLive:false` still holds: no image bytes, base64, path, declared media type or native dispatch option appears in any `live-v1` frame, retained result or replay.

A strict image admission failure is retained as `failed` with `image_admission_rejected` and the fixed message `Native image admission rejected (<native-code>)`, for the native admission-phase codes only. Pi raises those before it takes queue or run ownership, so no Pi-owned standard-user message, queue entry, input-image reference, provider call or success response was created and the client can release that submission and keep its local bytes. This describes Pi's own standard-user admission state; it is not a transactional rollback of the whole process. Extension or hook code that ran before the refusal owns its own side effects — unrelated file writes, network calls or external state are not undone. A failure Pi reports after accepting the submission is an ordinary run outcome on `live-v1` events; it never rewrites the accepted result and never triggers replay.

Two host rejections are also definitive, matched by exact whole message against Pi's own standard-user dispatch assertions and reported with fixed socket text: a stale dispatch target becomes `stale_target`, and a non-ready runtime (`replacing`, `failed`, `disposed`) becomes `runtime_unavailable`. Native prose is never echoed for a strict image dispatch. Anything else — an unrecognised error, an error whose text merely contains or resembles one of those phrases, a native error in the wrong phase, a non-`Error` value, or an error whose properties cannot be read — is reported as `uncertain` rather than as a definitive outcome. Non-strict commands keep their previous classification and bounded message echo unchanged.

### Native input-image projection

A complete supporting Pi host is detected only when all of these native surfaces are present together: the exact `inputImageCapability` v1 record, `inputImages`, `inputImageCoverage`, optional valid `inputImageHistoryCursor`, `pendingInputImages`, public `getCurrentBranchInputImage(...)`, and public `copyInputImageBytes(...)`. A legacy or partially upgraded host advertises none of this socket capability and does not accept the metadata query. No environment bypass enables partial support.

The `capabilities` result then includes Pi's exact native projection capability:

```json
{"inputImages":{"protocol":"pi.input-images","version":1,"messageBlock":"input_image_reference","metadataQuery":"input_image_history","maxImageBytes":33554432,"copyChunkBytes":262144,"maxPendingImages":256,"maxPendingDeclaredBytes":268435456,"maxPendingMetadataBytes":262144,"binaryInLive":false}}
```

Older metadata is read only through the pure native query:

```json
{"protocol":"live-v1","id":"input-images-1","op":"query","target":{"streamId":"...","sessionEpoch":0,"sessionId":"..."},"command":{"type":"input_image_history","cursor":{"leafId":"...","before":200},"maxItems":100,"maxBytes":262144}}
```

The validator accepts at most 500 records and 512 KiB and rejects unknown fields. Results bypass mutation retention like the other pure queries. Pi remains responsible for cursor freshness, branch scope, metadata and presentation projection.

Native `input_image_referenced`, `input_image_associated`, `input_image_unavailable`, `input_image_projection_limited`, and already-projected session events cross ordinary sequence/replay unchanged. The socket never walks message objects, recursively strips image-shaped records, invents IDs, reclassifies input images as output artifacts, or places image bytes/base64 on `live-v1`. The exported `InputImageCapableLiveTransportHost` exposes only Pi's fixed public direct current-branch metadata and bounded copy seams.

#### Private live input-image bytes

When the complete native host and Sesh transfer identity are available, `live-v1` separately advertises:

```json
{"inputImageBytes":{"protocol":"pi.input-image-read","version":1,"transport":"http+unix","socketSuffix":".input.sock","metadataLookup":"current-branch-id","maxImageBytes":33554432,"copyChunkBytes":262144,"maxConcurrentReads":2,"completeBeforeSuccess":true,"pendingReads":false,"binaryInLive":false}}
```

This does not add binary operations to `live-v1`. Sesh alone resolves the private target and listener generation from `consumer.json`, then sends strict HTTP `POST /v1/input-images/read` over the owner-only Unix socket. The request carries the exact thread, target, listener generation and input-image ID; a successful response repeats the generation in `X-Pi-Input-Image-Listener-Generation`. The browser supplies none of the private target, generation, path or owner fields.

The reader calls only `getCurrentBranchInputImage(target,id)` and hardened `copyInputImageBytes(...)`; there is no metadata paging fallback. Pending references, stale target, stale branch and stale listener generation are distinct failures. At most two reads run concurrently. Each read has a 12-second total deadline, a 32 MiB object limit and contiguous 256 KiB chunks. It buffers and verifies metadata, count and SHA-256 completely, repeats the direct metadata lookup, revalidates the marker target/generation, and only then emits HTTP success. Cancellation and lifecycle shutdown abort native copy. Socket directory/socket modes remain `0700`/`0600`, and cleanup compares the marker generation plus owned socket identity.

This byte reader only serves already-associated native input references. It is one of the prerequisites for `attachments.imageInput`, but it never reads pending images and submission remains a separate concern.

### Native output-artifact presentation

When Pi's snapshot carries the exact `pi.output-artifact-presentation` version-1 capability, `live-v1` advertises the independently gated transport capability:

```json
{"outputArtifactPresentation":{"protocol":"pi.output-artifact-presentation","version":1,"messageBlock":"output_artifact_reference","metadataQuery":"artifact_history","coverage":"direct_tool_result_content","states":["resolved","partial","missing","duplicate","unsupported"],"nestedToolResults":"unsupported_placeholder","binaryInLive":false}}
```

The detector accepts no unknown fields or variant literals and requires the ordered five-state tuple. Absence or any malformed field omits both `outputArtifactPresentation` and the `artifact_history` pure query; support is never inferred from artifact events, the copier, Pi version text, or Sesh configuration.

Older current-branch artifact metadata is available through the bounded native query:

```json
{"protocol":"live-v1","id":"artifacts-1","op":"query","target":{"streamId":"...","sessionEpoch":0,"sessionId":"..."},"command":{"type":"artifact_history","cursor":{"leafId":"...","before":200},"maxItems":100,"maxBytes":262144}}
```

The validator accepts at most 500 records and 512 KiB and rejects unknown fields. Results bypass mutation retention. Snapshots, active/branch history pages, current events, retained dispatch results and replay preserve Pi's canonical `output_artifact_reference` blocks unchanged. Pi projects every typed direct tool-result surface—including `turn_end.toolResults`—before native sizing and listener cloning. The socket does not traverse messages, reconstruct references, choose publications, or recursively strip image-shaped values; nested details, structured content, custom values, arguments and strings remain untouched.

### Managed output artifacts

When the live host exposes Pi's public `copyArtifactBytes(...)` and the complete Sesh transfer identity is valid, the ordinary extension also starts the bounded output publisher. `live-v1` then advertises:

```json
{"managedArtifacts":{"protocol":"sesh.pi-artifact-map","version":1,"delivery":"browser-query","maxArtifactBytes":33554432,"copyChunkBytes":262144,"binaryInLive":false}}
```

The publisher observes native `artifact_published` and `artifact_unavailable` records, reconciles bounded snapshot/history metadata, and uses the exact Sesh Unix routes `POST /v1/runtime/artifact-mappings/resolve`, `PUT /v1/runtime/artifact-mappings/content`, and `POST /v1/runtime/artifact-mappings/fail`. Both `client_id` and `mapping_id` are the same deterministic UUIDv8 derived from the complete `(threadId, nativeSessionId, publicationId)` key. Runtime success and error responses accept exactly schema 55 or 56 for the unchanged protocol `sesh.pi-artifact-map` version 1 shapes; other schemas remain invalid. The mapping union is exactly `staging|ready|unavailable|expired`; internal Sesh `pending` is never accepted on this runtime wire.

Pi remains authoritative for publication metadata and bytes; Sesh remains authoritative for durable mapping, browser authorization, expiry and retained bytes. Managed completion never creates, delays, rewrites or borrows a Pi event sequence. Native artifact events continue through ordinary replay immediately, while browsers resolve managed state separately by the complete thread/session/publication key. No artifact bytes, base64, managed paths or Sesh IDs enter `live-v1`. Legacy publications without an authoritative digest are resolved as durable `digest_unavailable` rather than being spooled or hashed by the extension.

Copies are streamed with HTTP backpressure in at most two concurrent 256 KiB chunks, with a 32 MiB object limit, a 64-publication queue, a 256-record/2 MiB disposable observation cache, bounded native-event buffering, 10-second native copy timeout and 15-second Sesh request timeout. Queue overflow schedules bounded metadata reconciliation instead of dropping native events or fabricating unavailability. Exact staging work can resume after stream/epoch rotation only within the same native session; the immutable producer target is never rewritten. Timeout, abort, stale target, transport loss and extension shutdown stay retryable for a later activation. Only the contract's narrow terminal copy/quota failures use the Sesh failure route.

Output publications remain strictly separate from inbound attachments. The publisher never scrapes tool details, arbitrary paths, model text or terminal output, and output artifact IDs are not accepted as input-image references.

Pi-owned interactive commands use the same dispatch operation, without a socket-side slash parser:

```json
{"protocol":"live-v1","id":"builtin-1","op":"dispatch","target":{"streamId":"...","sessionEpoch":0,"sessionId":"..."},"command":{"type":"interactive_builtin","operationId":"operation-1","command":"/model anthropic/example"}}
{"protocol":"live-v1","id":"answer-1","op":"dispatch","target":{"streamId":"...","sessionEpoch":0,"sessionId":"..."},"command":{"type":"interaction_response","interactionId":"...","response":{"kind":"confirm","confirmed":true}}}
{"protocol":"live-v1","id":"picker-1","op":"dispatch","target":{"streamId":"...","sessionEpoch":0,"sessionId":"..."},"command":{"type":"interaction_update","interactionId":"...","expectedRevision":7,"action":{"domain":"scoped_models","action":"persist"}}}
```

On Pi 1.4.0+ standard stateful pickers appear in `snapshot.pendingInteractions` with native `revision` values. `scoped_models` includes the complete model scope/refresh state; `settings` includes ordinary settings, model-thinking state and theme state. Native `interaction_updated` events carry the complete revised interaction and its `source`; the socket forwards and replays those records unchanged.

`interaction_update` is a mutation and is accepted only through `op:"dispatch"`. The validator requires a non-negative safe-integer `expectedRevision` and the exact domain-specific action shape: scoped-model `toggle`, `enable`, `clear`, `toggle_provider`, `move`, or `persist`; model-thinking `set` or `clear`; and theme `apply_single` or `apply_automatic`. Unknown or cross-action fields are rejected. Pi remains authoritative for target freshness, revision comparison, action semantics, validation and persistence. A stale revision returns Pi's `status:"stale"` result with the current interaction; the socket does not rewrite or retry it.

#### Extension-view actions

A Pi runtime that publishes extension-authored views adds one more action domain to the same `interaction_update` mutation:

```json
{"protocol":"live-v1","id":"view-1","op":"dispatch","target":{"streamId":"...","sessionEpoch":0,"sessionId":"..."},"command":{"type":"interaction_update","interactionId":"...","expectedRevision":3,"action":{"domain":"extension_view","viewId":"example-package/selector","action":{"kind":"choose","id":"anthropic/claude-sonnet-4-5"}}}}
```

The action object carries exactly `domain`, `viewId` and `action`. `viewId` is the view's static registration identity, at most 128 characters of `[a-z0-9][a-z0-9/-]*`; it is carried on the action so Pi can refuse a late action for a replaced view by identity. `action` is arbitrary JSON **data** chosen by the extension, so the socket bounds its size and shape and nothing else:

| Bound | Value |
|---|---|
| serialized action | 8 KiB |
| nesting depth | 8 |
| value nodes | 5,000 |
| string length | 8,192 characters |
| object key length | 64 characters |

The action value itself is depth 0 and a container's members are one level deeper, so **eight nested containers may wrap a scalar** and a ninth is refused. The byte bound is the exact serialized size, counted in UTF-8 bytes, so multi-byte text costs its encoded length.

Only plain JSON data is accepted: no accessor properties, array holes, symbol keys, foreign prototypes, non-finite numbers, functions, `undefined` or `bigint`, and no `__proto__`, `constructor` or `prototype` keys. Each container must also be reached exactly once, so a reference cycle and a shared object alias are both refused; parsed JSON can contain neither, so on the wire this only rejects values that could not have arrived as JSON. Every bound is enforced while walking the value — the action is never serialized whole first — and a breach is refused as `invalid_request` before the mutation ID is reserved and before Pi is called. Those refusals carry fixed text that never repeats the offending key, value or size.

The socket does not interpret an action's meaning, a view's controller policy, or which actions a view accepts; Pi owns all of that, including revision arbitration and which source wins. Extension-view update results are payload-free by the native contract: `{"status":"applied","revision":n}`, `{"status":"stale","revision":n}`, `{"status":"settled"}`, `{"status":"already_resolved"}`, `{"status":"not_found"}`, or `{"status":"invalid","error":"<fixed native prose>"}`. A retained result therefore never carries view state, and after `applied` or `stale` the current view comes from a live event or a fresh snapshot, never from a retained result. The socket forwards and retains whatever the runtime returns without inspecting its `status`.

Extension-view interaction lifecycle events are delivered to attached observers but never retained: `interaction_opened`, `interaction_updated` and `interaction_resolved` whose `interaction.kind` is exactly `extension_view` make their sequence a replay barrier, as transient auth notifications already do. A reconnect that would have to cross one receives an explicit `gap` plus a fresh snapshot, so extension-authored titles and state are served only from `snapshot.pendingInteractions` — the current authoritative state — and never recovered from transport memory. Every other interaction kind keeps its existing replay behaviour.

The wire validator exhaustively covers the public native `RpcCommand` union's field presence and JSON types, including images and optional fields. It also strictly validates interactive built-ins, every native interaction-response shape, standard-picker updates, both bounded transcript-history cursor types, and the bounded input-image and artifact metadata queries. It does not reproduce command semantics: cursor freshness, model/path/state checks and execution remain in Pi. Built-in command and interaction text remains ordinary JSON text, including valid multiline arguments.

`query` is the non-ledger path for native read-only work. Its base allowlist is `history`, `branch_history`, and the native `get_*` commands advertised by capabilities; `input_image_history` is added only for a complete native input-image host, and `artifact_history` only for an exact native output-presentation capability. Prompting, bash, queue/model/session changes, built-ins, interaction responses, interaction updates, and every other effectful command are rejected before Pi is called. Queries are forwarded to the same native dispatcher, but their IDs/results are not retained; a query may reuse its ID after completion, and `get_result` cannot retrieve it.

Server frames are `response`, `snapshot`, `event`, or `gap`:

```json
{"protocol":"live-v1","type":"response","id":"sub-1","ok":true,"result":{"subscriptionId":"sub-1","mode":"snapshot"}}
{"protocol":"live-v1","type":"snapshot","subscriptionId":"sub-1","snapshot":{"streamId":"...","sessionEpoch":0,"sessionId":"...","sequence":123}}
{"protocol":"live-v1","type":"event","subscriptionId":"sub-1","event":{"streamId":"...","sessionEpoch":0,"sessionId":"...","sequence":124,"type":"session_event"}}
{"protocol":"live-v1","type":"event","subscriptionId":"sub-1","event":{"streamId":"...","sessionEpoch":0,"sessionId":"...","sequence":125,"type":"interaction_updated","interaction":{"interactionId":"...","revision":8,"kind":"scoped_models","title":"Scoped models","browserAnswerable":true,"state":{"models":[],"enabledModelIds":null,"dirty":false,"refresh":{"status":"complete","message":"Current"}}},"source":"remote"}}
{"protocol":"live-v1","type":"gap","subscriptionId":"sub-1","gap":{"afterSequence":90,"availableFromSequence":100,"reason":"replay_unavailable"}}
```

A subscribe without `after` receives a coherent snapshot followed by events above its watermark. A resumable `after` receives retained events. A changed stream, sequence ahead of the host, evicted interval, or empty replay buffer older than the snapshot receives a gap and resnapshot.

Top-level native `auth_notification` events are deliberately transient. Current subscribers receive the exact notification, but the transport clears its retained replay suffix and advances the replay floor to that native sequence without storing the payload. A cursor older than the notification receives an explicit gap and authoritative snapshot; a cursor that already observed that sequence may replay later ordinary events. OAuth URLs, device codes and instructions never enter replay or the mutation-result ledger, and the transport fabricates no replacement native event.

Snapshots carry Pi's bounded recent active-context `messages`/`messageCoverage` and bounded canonical current-branch transcript coverage/cursors. Use `query` with `history` or `branch_history` to read older pages without consuming mutation retention. Both native page families default to at most 200 items and 512 KiB and accept bounds up to 500 items and 768 KiB. Every page identifies its scope and exact index/byte coverage, `hasOlder`/`hasNewer`, and an optional `nextCursor`. An item too large for the native bound is reported explicitly rather than truncated or counted as covered. A changed session leaf makes its cursor stale and requires a fresh snapshot.

### Correlation, limits and security

Dispatch IDs are recorded with their original stream/session target before the host is called. Reusing a retained mutation ID is refused. `get_result` requires that original target; a different stream, epoch, or session receives `result_target_mismatch`, while the correct old target can inspect its record after the live host moves on. Results are never silently retargeted and mutations are never automatically replayed.

Mutation result states are `running`, `completed`, `failed`, `unavailable`, or `uncertain`. `unavailable` with `outcome:"completed"` means native dispatch produced a result but the payload was unserializable, exceeded one frame, or could not fit the retention budget. It is deliberately not called failure or rollback; the client must reconcile the native state, and the known request ID remains duplicate-suppressed.

`uncertain` is the one state that reports an unresolved submission. It occurs only for a lease-prepared strict image dispatch (see above) whose host promise rejected before any result settled with something the socket cannot classify definitively: anything that is neither an exact native admission pair nor one of the exact standard-user refusal messages. A native error in the wrong phase, prose that merely contains a refusal phrase, a non-`Error` value and an error whose properties throw on read all land here. The record is exactly four fields:

```json
{"requestId":"<canonical-uuid>","status":"uncertain","reason":"native_dispatch_rejected","finishedAt":1762000000123}
```

`finishedAt` timestamps that host rejection, not agent completion or confirmed admission. The record carries no response, outcome, error, native code, phase, path or bytes, and the socket's own diagnostics do not echo the native exception text either. It is delivered in the ordinary successful envelope (`ok:true`), because delivery succeeded while the submission did not resolve: the request may or may not have reached the agent. Its bytes are part of the upfront result reservation, so it is retained even under result-capacity pressure, it is returned unchanged by target-scoped `get_result`, and the request ID stays duplicate-suppressed for the usual retention window. A client must not resend it; it must reconcile, keeping drafts and local bytes until the native state is known. A client that does not recognise the state should fail closed and treat the submission as unresolved rather than as a definitive failure.

Defaults are 16 clients, 16 concurrent pure queries, 1 MiB input lines, **1 MiB output frames/client queues** (matching the Sesh daemon), 2,048 replay events or 8 MiB, 256 mutation records, **8 MiB total serialized mutation-result retention**, and one-hour finished-result retention. Each admitted mutation is charged at least 4 KiB so a bounded failure record or completion tombstone always remains retainable. Completed payloads are retained as bounded serialized JSON rather than keeping arbitrary host object graphs. Slow clients are disconnected rather than consuming unbounded memory; reconnect then uses replay or an explicit gap/snapshot. Oversized events are represented as a gap plus snapshot, never silently truncated. The socket directory and live socket are owner-only (`0700`/`0600`). Local filesystem ownership is local authentication; Sesh or another proxy remains responsible for remote authorization. No unauthenticated network server is opened.

Lifecycle commands require reconciliation. `/new`, `/reload`, resume, fork, or another replacement may run native cleanup before the initiating live connection can observe its final outcome; cleanup can also discard that listener's in-memory result store. A client must discover the current owner-only socket again, take a fresh snapshot, and reconcile stream/session identity. It must not infer success from disconnect, label cleanup a deadlock, or blindly retry. `/new` normally rotates to a new session-named socket; `/reload` may recreate the same pathname with a new listener and stream.

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

## Explicit commands — protocol 2 (0.2.0)

Requires Pi **0.99.1+** (`@earendil-works/pi-coding-agent`). Update the package,
then `/reload` existing Pi sessions; new sessions load it automatically. The stable
`message` request also works on 0.1.0 runtimes, so message clients need no reload.

Messages are **literal** and steer without touching the editor. A leading slash is
not dispatched, templates are not expanded, and the ACK means **submitted**, not
model completion. To execute a command deliberately, use a separate operation:

```json
{"id":"discover-1","capabilities":true}
{"id":"compact-1","command":"/compact Preserve the implementation plan"}
{"id":"poll-1","getOperation":"compact-1"}
```

Every response echoes `id` when supplied. A request carries exactly one operation.
Command IDs are required (1–128 ASCII letters/digits/underscore/hyphen). A repeated
ID is refused, not executed again; use `getOperation`. Capabilities include
`protocol:2`, native `commands`, registered `resources`, session and tmux identity.

Capabilities also report whether the **live-v1** transport below is available on the
Pi that is actually running, so a client does not have to wait on a socket that will
never appear:

| `liveTransport` | Meaning |
|---|---|
| `unsupported` | This Pi has no interactive-ready capability. live-v1 is inert for the whole session and no `<sessionId>.live.sock` is created. Protocol 2 is unaffected — everything in this section works normally. |
| `supported` | The capability exists and live-v1 is registered. The live socket appears when the session becomes interactive. |
| `active` | The live socket is listening now. |

`unsupported` is a normal outcome, not a failure: a Pi without the capability is a
supported host for this extension, and the extension says so rather than logging a
startup line about a transport its user may never use.

Command/poll replies have `ok:true, operation:{status,result?,error?}`:

- **running**: not finished. Poll; never infer completion from the start ACK.
- **completed**: native API operation actually finished. Compaction only reaches
  this from Pi's `onComplete` callback, with the real compaction result.
- **failed**: native operation failed; `error` carries the reason. Empty-session
  compaction returns `Nothing to compact`, never `compacted:true`.
- **submitted**: a registered extension command/template/skill was handed to Pi's
  dispatcher. Its public `sendUserMessage` API returns void, so this is **NOT a claim
  that an async handler finished or succeeded**. Handler errors appear in Pi's UI.

Native commands: `/compact [instructions]` (idle/no queued messages/no other RPC
compaction required), `/model <pattern>` (no interactive picker), `/thinking [level]`
(off/minimal/low/medium/high/xhigh; result reports Pi's actual, possibly clamped level),
`/name [name]`, `/session` (session ID/name, cwd, model/thinking, context usage, idle
and queue state). Registered resources use the exact names from capabilities,
including `/skill:<name>`. Unknown commands refuse. Identity-changing operations
(new/resume/tree/fork/clone/import), reload, quit, auth and UI pickers are excluded.

Completed/submitted/failed operations are retained for **1 hour**, up to **256**
entries; live operations are never evicted. Capacity exhaustion refuses new work.
Reload/exit loses operation history. A disconnected or timed-out caller must inspect
its existing operation ID; **do not automatically retry an uncertain request**.

The legacy `{"compact":true,"instructions":"..."}` now replies only at completion
or error (may take minutes); prefer command + polling for bounded transports.

### Development

`npm install`, `npm test`, `npm run typecheck`. Unit tests isolate the public Pi API;
sesh's `thread.pi-rpc-send` matrix cells exercise the real extension inside real Pi,
with a viewer draft, a running tool, and a real compaction, locally and over SSH.
Set `PI_RPC_EXTENSION_UNDER_TEST=/absolute/path/index.ts` for sesh conformance runs
against a development checkout instead of the installed package.

The per-machine `package-lock.json` generated by Pi's git-package installer is
ignored, as distinct from extension source. This also lets existing installations
update without clobbering their already-generated untracked lockfile.
