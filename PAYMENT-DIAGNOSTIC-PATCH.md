# Payment diagnostics and form lifecycle — v0034

v0034 hardens FAST paid-upgrade volleys and keeps payment evidence for later debugging.

- Every prepared paid upgrade records `saved_id`, `form_id`, invoice `saved_id`, request `form_id`, request invoice `saved_id`, cost and preparation latency.
- FAST refuses to submit if the invoice/request points at a different saved gift, if `form_id` is missing, or if two paid plans share the same `form_id`.
- Telegram Stars forms are treated as a 10-minute resource: default background refresh starts at 300 seconds and FAST refuses forms older than 480 seconds.
- Refresh is staggered in small batches while LIVE is active, including inside the FAST hot zone, so a scanner that waits there for hours does not keep the form created on entry.
- Refresh failures and duplicate refreshed forms are fail-closed: the last plan is not silently replaced with a bad binding, and a stale/duplicate plan cannot pass FAST preflight.
- Completed-volley audit contains the payment binding fields, form age, send-start offset, status, actual number and error detail for every shot.
- Dedicated structured audit: `gift-hunter-v0034-payment-audit.jsonl`; `/log_full` exports only its last 24 hours, can split oversized ZIPs, and clears local history only after every part is sent successfully.
- No file logging, refresh or disk write is inserted between local FAST task creation and the prebuilt UDP FIRE broadcast.

Recommended live diagnostic after deployment: use volley size 2 on expendable gifts, verify two distinct `form_id` values and two confirmed results, then increase the volley size.

Hot-zone lifecycle hotfix: the old permanent form freeze was removed after a real run showed an armed form aging to 5330.599 seconds before the target arrived. `FAST_FORM_ARM_DISTANCE` now means “force-refresh and arm here”, not “stop maintenance here”.
