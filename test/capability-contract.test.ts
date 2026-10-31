import { test } from "node:test";
import assert from "node:assert/strict";
import {
	detectLiveImageAdmissionCapability,
	detectLiveInteractiveInputCapability,
	detectLiveViewResourceCapability,
} from "../live-transport.ts";

/**
 * Contract pins for every capability record this socket MIRRORS from its host.
 *
 * Why this file exists. Each of these records is checked with an exact-key test, so a
 * single wrong or missing field name makes detection return `undefined` for every real
 * runtime — the capability is never advertised, the feature is never reachable, and
 * nothing anywhere fails. That happened: `pi.view-resources` was first written with a
 * fifth field `page` when the host advertises `retention`, and all eleven of its own
 * tests passed, because they injected the fixture those tests defined and the fixture
 * encoded the same misreading as the implementation.
 *
 * So be clear about what this file does and does not do. **It does not detect host
 * drift.** Nothing here reads the host: the socket imports no runtime value from it
 * (every host-package import is `import type`), and the published package does not
 * export these constants. What it does is make the socket's claim about each record
 * EXPLICIT and single-sited, and name the producer declaration each was copied from —
 * so a reviewer has one place to compare against, and a socket-side edit cannot
 * quietly diverge from the source it claims to mirror.
 *
 * The real control is the practice, not the assertion: when a record changes, read the
 * producer. A green suite here means the socket still says what it said yesterday, not
 * that the host still agrees.
 */

/** Each record as its producer declares it, with where to verify it. */
const MIRRORED = [
	{
		protocol: "pi.view-resources",
		producer: "LIVE_RPC_VIEW_RESOURCE_CAPABILITY",
		record: {
			protocol: "pi.view-resources",
			version: 1,
			read: "view-resource-read-v1",
			staleness: "binding-stale-v1",
			retention: "requester-only",
		},
		detect: detectLiveViewResourceCapability,
	},
	{
		protocol: "pi.interactive-input",
		producer: "LIVE_RPC_INTERACTIVE_INPUT_CAPABILITY",
		record: {
			protocol: "pi.interactive-input",
			version: 1,
			input: "interactive-input-v1",
			literal: "expand-prompt-templates-v1",
			busy: "steer-default-v1",
			bash: "not-remote",
		},
		detect: detectLiveInteractiveInputCapability,
	},
	{
		protocol: "pi.image-admission",
		producer: "LIVE_RPC_IMAGE_ADMISSION_CAPABILITY",
		record: {
			protocol: "pi.image-admission",
			version: 1,
			preparation: "rpc-prompt-files-strict-v1",
			dispatch: "standard-user-strict-v1",
			requestValidation: "normalized-messages-v1",
			virtualModels: "stable-profile-v1",
			binaryInLive: false,
		},
		detect: detectLiveImageAdmissionCapability,
	},
] as const;

test("mirrored capability records match the key set and values this socket claims", () => {
	for (const { protocol, record, detect } of MIRRORED) {
		// The accepted record round-trips exactly, detached and frozen.
		const accepted = detect({ ...record } as Record<string, unknown>);
		assert.deepEqual(accepted, record, protocol);
		assert.equal(Object.isFrozen(accepted), true, protocol);

		// The key set itself is pinned. A field RENAMED on either side changes this
		// list, which is the failure the `page`/`retention` bug did not produce.
		const expectedKeys = Object.keys(record).sort();
		assert.deepEqual(Object.keys(accepted as object).sort(), expectedKeys, protocol);

		// One missing field, one extra field, and one wrong value are each refused —
		// so detection is exact in all three directions, not just on unknown keys.
		for (const key of Object.keys(record)) {
			const missing: Record<string, unknown> = { ...record };
			delete missing[key];
			assert.equal(detect(missing), undefined, `${protocol}: missing ${key}`);
			const wrong: Record<string, unknown> = { ...record, [key]: "__drifted__" };
			assert.equal(detect(wrong), undefined, `${protocol}: wrong ${key}`);
		}
		assert.equal(
			detect({ ...record, unexpected: 1 } as Record<string, unknown>),
			undefined,
			`${protocol}: extra field`,
		);
	}
});

test("a non-record is never mistaken for a capability", () => {
	for (const { protocol, detect } of MIRRORED) {
		for (const bad of [null, undefined, "", 0, 1, [], true, () => {}, new Date()]) {
			assert.equal(detect(bad as unknown as Record<string, unknown>), undefined, protocol);
		}
	}
});

/**
 * The two remaining mirrored records — `pi.input-images` and
 * `pi.output-artifact-presentation` — are detected by functions that are deliberately
 * NOT exported, because each needs more than its own record: the input-image detector
 * also requires two host methods and three snapshot arrays, and the artifact detector
 * requires the state list in order. Their records are pinned here as data so the key
 * sets are still single-sited and reviewable, and their behaviour is covered by
 * `input-image-reader.test.ts` and `managed-artifact-publisher.test.ts` against the
 * real transport.
 */
const MIRRORED_INTERNAL = {
	"pi.input-images": {
		producer: "LIVE_RPC_INPUT_IMAGE_CAPABILITY",
		keys: [
			"binaryInLive", "copyChunkBytes", "maxImageBytes", "maxPendingDeclaredBytes",
			"maxPendingImages", "maxPendingMetadataBytes", "messageBlock", "metadataQuery",
			"protocol", "version",
		],
	},
	"pi.output-artifact-presentation": {
		producer: "LIVE_RPC_OUTPUT_ARTIFACT_PRESENTATION_CAPABILITY",
		keys: [
			"binaryInLive", "coverage", "messageBlock", "metadataQuery",
			"nestedToolResults", "protocol", "states", "version",
		],
	},
} as const;

test("the internally-detected mirrored records have the key sets this socket claims", () => {
	assert.deepEqual(MIRRORED_INTERNAL["pi.input-images"].keys.length, 10);
	assert.deepEqual(MIRRORED_INTERNAL["pi.output-artifact-presentation"].keys.length, 8);
	// Sorted and duplicate-free, so the list is a set and a rename shows up as a diff.
	for (const [protocol, { keys }] of Object.entries(MIRRORED_INTERNAL)) {
		assert.deepEqual([...keys], [...keys].sort(), `${protocol} keys must be sorted`);
		assert.equal(new Set(keys).size, keys.length, `${protocol} keys must be unique`);
	}
});

/**
 * Records this socket OWNS rather than mirrors. There is no producer declaration to
 * drift from, so the hole above cannot apply to them — listed so that a future reader
 * does not go looking for a host constant to match. `pi.input-image-read` is the
 * trap: the `pi.` prefix makes it look mirrored, and it is not.
 */
test("socket-owned protocols are not host records and have no producer to mirror", async () => {
	const { ATTACHMENT_CAPABILITY } = await import("../attachment-transfer.ts");
	const { LIVE_INPUT_IMAGE_READER_CAPABILITY } = await import("../input-image-reader.ts");
	const { MANAGED_ARTIFACT_CAPABILITY } = await import("../managed-artifact-publisher.ts");
	assert.equal(ATTACHMENT_CAPABILITY.protocol, "sesh.pi-transfer");
	assert.equal(LIVE_INPUT_IMAGE_READER_CAPABILITY.protocol, "pi.input-image-read");
	assert.equal(MANAGED_ARTIFACT_CAPABILITY.protocol, "sesh.pi-artifact-map");
});
