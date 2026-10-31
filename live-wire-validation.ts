import type { RpcCommand } from "@earendil-works/pi-coding-agent";

export type InvalidWireRequest = (message: string) => never;
type WireRecord = Record<string, unknown>;
type FieldValidator = (value: unknown, field: string, invalid: InvalidWireRequest) => void;
type CommandValidator = (command: WireRecord, invalid: InvalidWireRequest) => void;

const THINKING_LEVELS = new Set(["off", "minimal", "low", "medium", "high", "xhigh", "max"]);
const QUEUE_MODES = new Set(["all", "one-at-a-time"]);
const STREAMING_BEHAVIORS = new Set(["steer", "followUp"]);
const MAX_HISTORY_MESSAGES = 500;
const MAX_HISTORY_BYTES = 768 * 1024;
const MAX_INPUT_IMAGE_ITEMS = 500;
const MAX_INPUT_IMAGE_BYTES = 512 * 1024;
const MAX_ARTIFACT_ITEMS = 500;
const MAX_ARTIFACT_BYTES = 512 * 1024;

/**
 * The extension-view action boundary, mirroring Pi's own JSON bounds so the first
 * boundary the browser's bytes cross is not unbounded. The socket bounds size and shape
 * only: it never interprets an action's meaning, its view's controller policy, or which
 * actions a view accepts.
 */
const EXTENSION_VIEW_ID_PATTERN = /^[a-z0-9][a-z0-9/-]*$/;
const MAX_EXTENSION_VIEW_ID_CHARS = 128;
const MAX_EXTENSION_ACTION_BYTES = 8 * 1024;
/**
 * The action value itself is depth 0 and a container's members are one level deeper, so
 * eight nested containers may wrap a scalar. This matches Pi's convention exactly; the
 * socket must not be one level stricter than the runtime that owns the semantics.
 */
const MAX_EXTENSION_ACTION_DEPTH = 8;
const MAX_EXTENSION_ACTION_NODES = 5000;
const MAX_EXTENSION_ACTION_STRING_CHARS = 8192;
const MAX_EXTENSION_ACTION_KEY_CHARS = 64;
/** `JSON.parse` creates own enumerable properties for these, so they must be refused. */
const FORBIDDEN_JSON_KEYS: ReadonlySet<string> = new Set(["__proto__", "constructor", "prototype"]);

function isRecord(value: unknown): value is WireRecord {
	return value !== null && typeof value === "object" && !Array.isArray(value);
}

function exactFields(
	value: WireRecord,
	required: Record<string, FieldValidator>,
	optional: Record<string, FieldValidator>,
	invalid: InvalidWireRequest,
	includeType = true,
): void {
	const allowed = new Set([...(includeType ? ["type"] : []), ...Object.keys(required), ...Object.keys(optional)]);
	for (const key of Object.keys(value)) {
		if (!allowed.has(key)) invalid(`unknown command field: ${key}`);
	}
	for (const [field, validate] of Object.entries(required)) {
		if (!Object.hasOwn(value, field)) invalid(`missing command field: ${field}`);
		validate(value[field], `command.${field}`, invalid);
	}
	for (const [field, validate] of Object.entries(optional)) {
		if (Object.hasOwn(value, field)) validate(value[field], `command.${field}`, invalid);
	}
}

const stringField: FieldValidator = (value, field, invalid) => {
	if (typeof value !== "string") invalid(`${field} must be text`);
};

const booleanField: FieldValidator = (value, field, invalid) => {
	if (typeof value !== "boolean") invalid(`${field} must be boolean`);
};

function enumField(values: ReadonlySet<string>): FieldValidator {
	return (value, field, invalid) => {
		if (typeof value !== "string" || !values.has(value)) invalid(`${field} has an unsupported value`);
	};
}

function integerField(minimum: number, maximum: number): FieldValidator {
	return (value, field, invalid) => {
		if (!Number.isSafeInteger(value) || (value as number) < minimum || (value as number) > maximum) {
			invalid(`${field} must be an integer from ${minimum} to ${maximum}`);
		}
	};
}

const nullableLeafIdField: FieldValidator = (value, field, invalid) => {
	if (value === null) return;
	if (
		typeof value !== "string" ||
		value.length === 0 ||
		value.length > 512 ||
		/[\u0000-\u001f\u007f]/.test(value)
	) {
		invalid(`${field} must be null or 1-512 text characters without controls`);
	}
};

const imagesField: FieldValidator = (value, field, invalid) => {
	if (!Array.isArray(value)) return invalid(`${field} must be an array`);
	for (let index = 0; index < value.length; index++) {
		const image = value[index];
		if (!isRecord(image)) return invalid(`${field}[${index}] must be an object`);
		const keys = Object.keys(image);
		if (keys.length !== 3 || !keys.includes("type") || !keys.includes("data") || !keys.includes("mimeType")) {
			invalid(`${field}[${index}] must contain exactly type, data and mimeType`);
		}
		if (image.type !== "image") invalid(`${field}[${index}].type must be image`);
		if (typeof image.data !== "string") invalid(`${field}[${index}].data must be text`);
		if (typeof image.mimeType !== "string") invalid(`${field}[${index}].mimeType must be text`);
	}
};

const optionalId = { id: stringField };
const noFields = (command: WireRecord, invalid: InvalidWireRequest) => exactFields(command, {}, optionalId, invalid);
const required = (fields: Record<string, FieldValidator>, optional: Record<string, FieldValidator> = optionalId): CommandValidator =>
	(command, invalid) => exactFields(command, fields, optional, invalid);

/**
 * Exhaustive structural validators for Pi's public RpcCommand union.
 * These validate JSON field presence/types only; Pi remains the sole owner of
 * command semantics, state checks, path/model resolution and execution.
 */
const RPC_COMMAND_VALIDATORS = {
	prompt: required(
		{ message: stringField },
		{
			...optionalId,
			images: imagesField,
			streamingBehavior: enumField(STREAMING_BEHAVIORS),
			// Verbatim pass-through so a slash-shaped line can be sent as TEXT.
			// The socket only checks the type; Pi owns what expansion means.
			expandPromptTemplates: booleanField,
		},
	),
	steer: required({ message: stringField }, { ...optionalId, images: imagesField }),
	follow_up: required({ message: stringField }, { ...optionalId, images: imagesField }),
	abort: noFields,
	clear_queue: noFields,
	new_session: required({}, { ...optionalId, parentSession: stringField }),
	get_state: noFields,
	set_model: required({ provider: stringField, modelId: stringField }),
	cycle_model: noFields,
	get_available_models: noFields,
	set_thinking_level: required({ level: enumField(THINKING_LEVELS) }),
	cycle_thinking_level: noFields,
	get_available_thinking_levels: noFields,
	set_steering_mode: required({ mode: enumField(QUEUE_MODES) }),
	set_follow_up_mode: required({ mode: enumField(QUEUE_MODES) }),
	compact: required({}, { ...optionalId, customInstructions: stringField }),
	set_auto_compaction: required({ enabled: booleanField }),
	set_auto_retry: required({ enabled: booleanField }),
	abort_retry: noFields,
	bash: required({ command: stringField }, { ...optionalId, excludeFromContext: booleanField }),
	abort_bash: noFields,
	get_session_stats: noFields,
	export_html: required({}, { ...optionalId, outputPath: stringField }),
	switch_session: required({ sessionPath: stringField }),
	fork: required({ entryId: stringField }),
	clone: noFields,
	get_fork_messages: noFields,
	get_entries: required({}, { ...optionalId, since: stringField }),
	get_tree: noFields,
	get_last_assistant_text: noFields,
	set_session_name: required({ name: stringField }),
	get_messages: noFields,
	get_commands: noFields,
} satisfies Record<RpcCommand["type"], CommandValidator>;

function validateInteractionResponse(command: WireRecord, invalid: InvalidWireRequest): void {
	exactFields(command, { interactionId: stringField, response: (value, field) => {
		if (!isRecord(value)) invalid(`${field} must be an object`);
		switch (value.kind) {
			case "select":
				exactFields(
					value,
					{ kind: enumField(new Set(["select"])), optionId: stringField },
					{ actionId: stringField },
					invalid,
					false,
				);
				break;
			case "confirm":
				exactFields(value, { kind: enumField(new Set(["confirm"])), confirmed: booleanField }, {}, invalid, false);
				break;
			case "input":
				exactFields(value, { kind: enumField(new Set(["input"])), value: stringField }, {}, invalid, false);
				break;
			case "editor":
				exactFields(value, { kind: enumField(new Set(["editor"])), value: stringField }, {}, invalid, false);
				break;
			case "setting":
				exactFields(
					value,
					{ kind: enumField(new Set(["setting"])), settingId: stringField, value: stringField },
					{},
					invalid,
					false,
				);
				break;
			case "complete":
				exactFields(value, { kind: enumField(new Set(["complete"])) }, {}, invalid, false);
				break;
			case "cancel":
				exactFields(value, { kind: enumField(new Set(["cancel"])) }, {}, invalid, false);
				break;
			default:
				invalid(`${field}.kind has an unsupported value`);
		}
	} }, {}, invalid);
}

/**
 * Walk one inbound extension-view action, enforcing the JSON boundary while measuring its
 * serialized size incrementally. Nothing is serialized whole first: depth, node count,
 * key length, string length and the running byte total are each checked as the walk
 * reaches a node, so a hostile deep, wide or huge value is refused before any recursive
 * serialization. Only plain JSON data is accepted — no accessors, holes, symbol keys,
 * foreign prototypes, non-finite numbers, functions, `undefined` or `bigint`.
 *
 * Every container must be reached exactly once: a reference cycle and a shared object
 * alias are both refused, matching Pi's own rule. Parsed wire JSON can contain neither,
 * so on the live path this only ever rejects a value that could not have arrived as JSON.
 */
function measureBoundedActionJson(
	value: unknown,
	depth: number,
	budget: { bytes: number; nodes: number },
	seen: Set<object>,
	malformed: () => never,
	oversized: () => never,
): void {
	if (depth > MAX_EXTENSION_ACTION_DEPTH) oversized();
	budget.nodes += 1;
	if (budget.nodes > MAX_EXTENSION_ACTION_NODES) oversized();
	const spend = (bytes: number): void => {
		budget.bytes += bytes;
		if (budget.bytes > MAX_EXTENSION_ACTION_BYTES) oversized();
	};
	if (value === null) {
		spend(4);
		return;
	}
	switch (typeof value) {
		case "boolean":
			spend(value ? 4 : 5);
			return;
		case "number":
			if (!Number.isFinite(value)) malformed();
			spend(String(value).length);
			return;
		case "string":
			// Bound the length before encoding this one string.
			if (value.length > MAX_EXTENSION_ACTION_STRING_CHARS) oversized();
			spend(Buffer.byteLength(JSON.stringify(value)));
			return;
		case "object":
			break;
		default:
			malformed();
	}
	const container = value as object;
	if (seen.has(container)) malformed();
	seen.add(container);
	if (Array.isArray(container)) {
		if (Object.getPrototypeOf(container) !== Array.prototype) malformed();
		spend(2 + Math.max(0, container.length - 1));
		for (let index = 0; index < container.length; index += 1) {
			const descriptor = Object.getOwnPropertyDescriptor(container, index);
			if (!descriptor || !("value" in descriptor)) malformed();
			measureBoundedActionJson(descriptor.value, depth + 1, budget, seen, malformed, oversized);
		}
		return;
	}
	const prototype = Object.getPrototypeOf(container);
	if (prototype !== Object.prototype && prototype !== null) malformed();
	if (Object.getOwnPropertySymbols(container).length > 0) malformed();
	const keys = Object.keys(container);
	spend(2 + Math.max(0, keys.length - 1));
	for (const key of keys) {
		if (key.length > MAX_EXTENSION_ACTION_KEY_CHARS) oversized();
		if (FORBIDDEN_JSON_KEYS.has(key)) malformed();
		const descriptor = Object.getOwnPropertyDescriptor(container, key)!;
		if (!("value" in descriptor)) malformed();
		spend(Buffer.byteLength(JSON.stringify(key)) + 1);
		measureBoundedActionJson(descriptor.value, depth + 1, budget, seen, malformed, oversized);
	}
}

const extensionViewIdField: FieldValidator = (value, field, invalid) => {
	if (typeof value !== "string" || value.length < 1 || value.length > MAX_EXTENSION_VIEW_ID_CHARS
		|| !EXTENSION_VIEW_ID_PATTERN.test(value)) {
		invalid(`${field} must be a bounded lower-case view identity`);
	}
};

/** Fixed diagnostics only: never the offending key, value, path or size. */
const extensionViewActionField: FieldValidator = (value, field, invalid) => {
	measureBoundedActionJson(
		value,
		0,
		{ bytes: 0, nodes: 0 },
		new Set(),
		() => invalid(`${field} must be plain JSON data`),
		() => invalid(`${field} exceeds the extension view action bounds`),
	);
};

function validateInteractionUpdateAction(value: unknown, field: string, invalid: InvalidWireRequest): void {
	if (!isRecord(value)) invalid(`${field} must be an object`);
	if (typeof value.domain !== "string") invalid(`${field}.domain must be text`);
	// The extension-view domain carries a bounded JSON action object instead of a string
	// action name, so it is matched before the existing string-action domains, whose
	// validation is unchanged.
	if (value.domain === "extension_view") {
		exactFields(
			value,
			{
				domain: enumField(new Set(["extension_view"])),
				viewId: extensionViewIdField,
				action: extensionViewActionField,
			},
			{},
			invalid,
			false,
		);
		return;
	}
	if (typeof value.action !== "string") invalid(`${field}.action must be text`);
	if (value.domain === "scoped_models") {
		switch (value.action) {
			case "toggle":
				exactFields(
					value,
					{ domain: enumField(new Set(["scoped_models"])), action: enumField(new Set(["toggle"])), modelId: stringField },
					{},
					invalid,
					false,
				);
				return;
			case "enable":
			case "clear":
				exactFields(
					value,
					{ domain: enumField(new Set(["scoped_models"])), action: enumField(new Set(["enable", "clear"])) },
					{ query: stringField },
					invalid,
					false,
				);
				return;
			case "toggle_provider":
				exactFields(
					value,
					{
						domain: enumField(new Set(["scoped_models"])),
						action: enumField(new Set(["toggle_provider"])),
						provider: stringField,
					},
					{},
					invalid,
					false,
				);
				return;
			case "move":
				exactFields(
					value,
					{
						domain: enumField(new Set(["scoped_models"])),
						action: enumField(new Set(["move"])),
						modelId: stringField,
						direction: enumField(new Set(["up", "down"])),
					},
					{},
					invalid,
					false,
				);
				return;
			case "persist":
				exactFields(
					value,
					{ domain: enumField(new Set(["scoped_models"])), action: enumField(new Set(["persist"])) },
					{},
					invalid,
					false,
				);
				return;
			default:
				invalid(`${field}.action has an unsupported scoped_models value`);
		}
	}
	if (value.domain === "model_thinking") {
		switch (value.action) {
			case "set":
				exactFields(
					value,
					{
						domain: enumField(new Set(["model_thinking"])),
						action: enumField(new Set(["set"])),
						modelId: stringField,
						level: stringField,
					},
					{},
					invalid,
					false,
				);
				return;
			case "clear":
				exactFields(
					value,
					{ domain: enumField(new Set(["model_thinking"])), action: enumField(new Set(["clear"])), modelId: stringField },
					{},
					invalid,
					false,
				);
				return;
			default:
				invalid(`${field}.action has an unsupported model_thinking value`);
		}
	}
	if (value.domain === "theme") {
		switch (value.action) {
			case "apply_single":
				exactFields(
					value,
					{ domain: enumField(new Set(["theme"])), action: enumField(new Set(["apply_single"])), theme: stringField },
					{},
					invalid,
					false,
				);
				return;
			case "apply_automatic":
				exactFields(
					value,
					{
						domain: enumField(new Set(["theme"])),
						action: enumField(new Set(["apply_automatic"])),
						lightTheme: stringField,
						darkTheme: stringField,
					},
					{},
					invalid,
					false,
				);
				return;
			default:
				invalid(`${field}.action has an unsupported theme value`);
		}
	}
	invalid(`${field}.domain has an unsupported value`);
}

function validateInteractionUpdate(command: WireRecord, invalid: InvalidWireRequest): void {
	exactFields(
		command,
		{
			interactionId: stringField,
			expectedRevision: integerField(0, Number.MAX_SAFE_INTEGER),
			action: validateInteractionUpdateAction,
		},
		{},
		invalid,
	);
}

/**
 * `interactive_input` hands ONE raw input line to the host's own interpreter. The
 * socket validates field shape only: it does not parse the line, does not look for a
 * leading "/" or "!", and does not decide whether the line is a command. A line that
 * looks like a command and a line that does not are the same thing here.
 *
 * It is a MUTATION: it belongs to `op: "dispatch"` with the ordinary retained result
 * record, and is never added to the query command set.
 */
function validateInteractiveInput(command: WireRecord, invalid: InvalidWireRequest): void {
	exactFields(
		command,
		{ text: stringField },
		{
			...optionalId,
			images: imagesField,
			// Same accepted values as `streamingBehavior`; reused so the two
			// cannot drift apart. Pi's default when absent is "steer".
			busy: enumField(STREAMING_BEHAVIORS),
			literal: booleanField,
		},
		invalid,
	);
}

/**
 * The page ceiling the read contract fixes. A `maxBytes` above it is REFUSED, not
 * clamped: silently returning a smaller page than asked for is indistinguishable, to
 * the caller, from a short final page, so it would turn a caller's mistake into a
 * wrong pagination decision.
 */
const VIEW_RESOURCE_PAGE_MAX_BYTES = 32 * 1024;
const VIEW_RESOURCE_NAMES: ReadonlySet<string> = new Set(["output", "input", "diagnostics"]);

/**
 * One read of one view's resource. A pure QUERY: it retains nothing, emits no event
 * and cannot be replayed, so its `id` is correlation only.
 *
 * Every member of `ref` is required. A read is never served on partial identity:
 * `viewId`/`viewVersion` say which view and which generation of it, `expectedRevision`
 * and `datasetId` say which state the caller believes it is reading, and the host
 * answers `stale` — carrying no rows — if either has moved. Accepting a read with any
 * of them missing would mean serving rows against an identity nobody asserted.
 *
 * `command.interactionId` must equal `ref.interactionId`, checked here at the wire
 * before any host call, so one view cannot borrow another's authority.
 *
 * `cursor` is bounded by the SAME walker and budget as an extension-view action
 * (8 KiB, depth 8, 5000 nodes, 64-char keys): no prototype-pollution key, accessor,
 * cycle, shared alias or non-finite number, and the bound is enforced while walking
 * rather than by serializing first.
 */
function validateViewResourceRead(command: WireRecord, invalid: InvalidWireRequest): void {
	exactFields(
		command,
		{
			interactionId: stringField,
			ref: (value, field) => {
				if (!isRecord(value)) invalid(`${field} must be an object`);
				exactFields(
					value as WireRecord,
					{
						interactionId: stringField,
						viewId: extensionViewIdField,
						viewVersion: integerField(0, Number.MAX_SAFE_INTEGER),
						expectedRevision: integerField(0, Number.MAX_SAFE_INTEGER),
						// Opaque to the socket: bounded, non-empty, never parsed.
						datasetId: (inner, innerField) => {
							if (typeof inner !== "string" || inner.length < 1 || inner.length > 512
								|| /[\u0000-\u001f\u007f]/.test(inner)) {
								invalid(`${innerField} must be 1-512 text characters without controls`);
							}
						},
					},
					{},
					invalid,
					false,
				);
			},
			resource: enumField(VIEW_RESOURCE_NAMES),
			cursor: extensionViewActionField,
		},
		{ ...optionalId, maxBytes: integerField(1, VIEW_RESOURCE_PAGE_MAX_BYTES) },
		invalid,
	);
	const ref = command.ref;
	if (isRecord(ref) && ref.interactionId !== command.interactionId) {
		invalid("command.interactionId must equal command.ref.interactionId");
	}
}

function validateHistory(command: WireRecord, invalid: InvalidWireRequest): void {
	exactFields(
		command,
		{
			cursor: (value, field) => {
				if (!isRecord(value)) invalid(`${field} must be an object`);
				exactFields(
					value,
					{
						leafId: nullableLeafIdField,
						before: integerField(0, Number.MAX_SAFE_INTEGER),
					},
					{},
					invalid,
					false,
				);
			},
		},
		{
			maxMessages: integerField(1, MAX_HISTORY_MESSAGES),
			maxBytes: integerField(2, MAX_HISTORY_BYTES),
		},
		invalid,
	);
}

function validateBranchHistory(command: WireRecord, invalid: InvalidWireRequest): void {
	exactFields(
		command,
		{
			cursor: (value, field) => {
				if (!isRecord(value)) invalid(`${field} must be an object`);
				exactFields(
					value,
					{
						leafId: nullableLeafIdField,
						before: integerField(0, Number.MAX_SAFE_INTEGER),
					},
					{},
					invalid,
					false,
				);
			},
		},
		{
			maxItems: integerField(1, MAX_HISTORY_MESSAGES),
			maxBytes: integerField(2, MAX_HISTORY_BYTES),
		},
		invalid,
	);
}

function validateArtifactHistory(command: WireRecord, invalid: InvalidWireRequest): void {
	exactFields(
		command,
		{
			cursor: (value, field) => {
				if (!isRecord(value)) invalid(`${field} must be an object`);
				exactFields(
					value,
					{
						leafId: nullableLeafIdField,
						before: integerField(0, Number.MAX_SAFE_INTEGER),
					},
					{},
					invalid,
					false,
				);
			},
		},
		{
			maxItems: integerField(1, MAX_ARTIFACT_ITEMS),
			maxBytes: integerField(2, MAX_ARTIFACT_BYTES),
		},
		invalid,
	);
}

function validateInputImageHistory(command: WireRecord, invalid: InvalidWireRequest): void {
	exactFields(
		command,
		{
			cursor: (value, field) => {
				if (!isRecord(value)) invalid(`${field} must be an object`);
				exactFields(
					value,
					{
						leafId: nullableLeafIdField,
						before: integerField(0, Number.MAX_SAFE_INTEGER),
					},
					{},
					invalid,
					false,
				);
			},
		},
		{
			maxItems: integerField(1, MAX_INPUT_IMAGE_ITEMS),
			maxBytes: integerField(2, MAX_INPUT_IMAGE_BYTES),
		},
		invalid,
	);
}

export function validateLiveWireCommand(command: WireRecord, invalid: InvalidWireRequest): void {
	if (typeof command.type !== "string") invalid("command.type must be text");
	if (command.type === "interactive_builtin") {
		exactFields(command, { operationId: stringField, command: stringField }, {}, invalid);
		return;
	}
	if (command.type === "interaction_response") {
		validateInteractionResponse(command, invalid);
		return;
	}
	if (command.type === "interaction_update") {
		validateInteractionUpdate(command, invalid);
		return;
	}
	if (command.type === "interactive_input") {
		validateInteractiveInput(command, invalid);
		return;
	}
	if (command.type === "view_resource_read") {
		validateViewResourceRead(command, invalid);
		return;
	}
	if (command.type === "history") {
		validateHistory(command, invalid);
		return;
	}
	if (command.type === "branch_history") {
		validateBranchHistory(command, invalid);
		return;
	}
	if (command.type === "artifact_history") {
		validateArtifactHistory(command, invalid);
		return;
	}
	if (command.type === "input_image_history") {
		validateInputImageHistory(command, invalid);
		return;
	}
	const validator = RPC_COMMAND_VALIDATORS[command.type as RpcCommand["type"]];
	if (!validator) invalid(`unsupported command type: ${command.type}`);
	validator(command, invalid);
}
