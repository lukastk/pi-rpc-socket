# pi-rpc-socket

**Read `README.md` first** — it is the real documentation for this repo: the protocol
table, the per-session socket path and the `sun_path` length reasoning behind it, and
the lifecycle on `session_start` / `session_shutdown`.

Four things the README does not cover, because they are about how this repo is
consumed rather than what it does:

- **This clone is not what runs.** myagent declares
  `git:github.com/lukastk/pi-rpc-socket` in its `external_extensions.txt` and its
  installer runs `pi install` then a targeted `pi update` for each declared source,
  so the extension is fetched from GitHub. Editing this working copy changes nothing
  in a live Pi until the work is pushed and that update runs (then `/reload`).
- **It is a mymain-only development checkout.** `~/mysetup/pi-rpc-socket` is
  deliberately absent from `MYSETUP_REPOS` in `myrig/scripts/installs/mysetup.sh`,
  so it is never cloned to the other machines. A cross-machine report calling it
  "missing" on macstudio, macbook, ideapad, pocket4 or pixel9 is expected, not a
  fault to fix.
- **mypi carries its own frozen copy, from the `live-v1` branch.** mypi's bundle embeds
  a snapshot pinned by `mypi/packaging/pins/socket-tree.json`. It comes from `live-v1`,
  which adds the live transport (`<session>.live.sock`) and is not merged into `main`.
  Active `live-v1` work happens in a box checkout (the pin's recorded `source`), not in
  this clone. Changing what mypi runs means updating `live-v1`, regenerating that pin,
  and building and publishing a new mypi bundle (mypi `packaging/README.md`).
- **Public repo**, unlike most of the my-suite (`package.json` declares MIT; there is
  no LICENSE file yet). Nothing machine-specific, private or credential-shaped
  belongs in it.

Checks: `npm test` (node's test runner over `test/*.test.ts`) and `npm run typecheck`.

Protocol changes have out-of-repo consumers that pin a version — sesh's delivery path
(`sesh/skills/sesh-cli/references/delivery.md`) and myagent's `self-compact` skill both
reference pi-rpc-socket 0.2.0 / protocol 2 — and mypi's `packaging/pins/socket-tree.json`
pins an exact `live-v1` payload. Bump deliberately and update those callers.
