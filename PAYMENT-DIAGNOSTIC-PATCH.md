# v0037 PREPAID FAST diagnostic patch

This release replaces the v0036 parallel Stars-payment experiment. LIVE activation sequentially prepays each selected saved gift with `InputInvoiceStarGiftPrepaidUpgrade`, re-reads Telegram state after every payment, and fails closed if `gift_num` appears or the gift becomes unique. The exact-trigger path is then limited to an unordered burst of already-prepaid `UpgradeStarGiftRequest` objects.

Key audit events: `prepaid_upgrade_prepare_started`, `prepaid_upgrade_submit_started`, `prepaid_upgrade_confirmed`, `prepaid_volley_progress`, `prepaid_volley_ready`, plus `fast_payment_batch_*` carrying `request_type`.

---

# Payment diagnostics and FAST volley — v0037

v0037 hardens multi-payment Stars volleys after the production `#444665` run.

## Что изменено

- Формы по-прежнему живут по политике `300 с` дальше 50 номеров и `120 с` внутри 50; на входе в near-target зону весь комплект refresh-ится принудительно.
- Каждый paid plan проверяется по `saved_id`, invoice binding, `form_id`, request binding и возрасту формы. Повторяющийся `form_id` блокирует залп fail-closed.
- Локальный залп теперь ставится одним `client._sender.send([requests...], ordered=False)` burst. Все RequestState попадают в sender в одном event-loop turn до первого ожидания ответа.
- `invokeAfterMsg`/ordered dependency удалены, потому что реальный залп показал задержку второго submit на 521.728 мс.
- Финансовый batch не использует client-level request retry. Неоднозначный результат не вызывает автоматический повтор платежа.
- `FORM_SUBMIT_DUPLICATE`, сетевой сбой с неоднозначным состоянием и прочие результаты, где payment мог быть выполнен, автоматически не повторяются. Они проверяются и при необходимости оставляют payment hold.
- `gift-hunter-v0037-payment-audit.jsonl` пишет `fast_payment_batch_started`, `fast_payment_batch_phase`, `fast_payment_batch_finished`, binding-поля, возраст формы, send-start и итог каждого экземпляра.
- В batch audit пишется не секретный snapshot MTProto: `client_epoch`, `connect_epoch`, `connected`, `dc_id`, возраст клиента/соединения и `sender_reconnecting`.
- Между созданием локальной batch-task и prebuilt UDP FIRE по-прежнему нет await/log/disk I/O.

## Разбор production-сбоя v0034

Обе формы перед выстрелом были свежими (~72–73 с). Два payment RPC стартовали через `1.769` и `1.857` мс после trigger; один вернул `FORM_SUBMIT_DUPLICATE`, второй подтвердил `#444665`. Поэтому этот hotfix меняет именно механизм multi-submit, а не интервалы refresh.

Уведомление Telegram о новом устройстве содержит server timestamp `02:48:18 UTC`. В этот период лог действительно показывает первоначальные connect/auth действия MTProto. Выстрел произошёл около `11:01:22 UTC`; рядом с ним нет connect/disconnect/reconnect строк, то есть в том выстреле сессия не отваливалась.

Offline validation: `198` tests `OK`; живой Stars-залп в тестовой среде не выполнялся.
