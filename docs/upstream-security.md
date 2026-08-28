# Upstream security backport policy

This fork is based on upstream quic-go v0.49.0 plus local transport changes. It does not claim to be an upstream v0.61 branch.

The following advisories were reviewed against this baseline on 2026-08-28:

- GHSA-3q6m-v84f-6p9h / CVE-2023-46239: fixed before v0.49.0.
- GHSA-c33x-xqrf-c478 / CVE-2024-22189: fixed before v0.49.0.
- GHSA-j972-j939-p2v3 / CVE-2025-29785: only upstream v0.50.0 is affected.
- GHSA-ppxx-5m9h-6vxf / CVE-2023-49295: fixed before v0.49.0.
- GHSA-px8v-pp82-rcvr / CVE-2024-53259: fixed before v0.49.0.
- GHSA-47m2-4cr7-mhcw / CVE-2025-59530: backported from upstream handshake-confirmation key discard behavior, with an idempotence guard.
- GHSA-g754-hx8w-x2g6 / CVE-2025-64702: backported using qpack v0.6 incremental decoding and decoded field-section limits.
- GHSA-vvgj-x9jq-8cj9 / CVE-2026-40898: the same decoded limit is enforced for response and request trailers.

`.github/workflows/upstream-security.yml` compares the upstream advisory list and latest release with the reviewed state files. A new advisory or release deliberately fails the scheduled workflow until its applicability and required backports are recorded here.

The last reviewed upstream release is v0.61.0. Updating that marker is an audit decision, not a claim that this fork contains every v0.61 change.
