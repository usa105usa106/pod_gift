# Payment diagnostics and FAST volley — v0035

v0035 hardens multi-payment Stars volleys after the production `#444665` run.

## Что изменено

- Формы по-прежнему живут по политике `300 с` дальше 50 номеров и `120 с` внутри 50; на входе в near-target зону весь комплект refresh-ится принудительно.
- Каждый paid plan проверяется по `saved_id`, invoice binding, `form_id`, request binding и возрасту формы. Повторяющийся `form_id` блокирует залп fail-closed.
- Локальный залп теперь отправляется одним `client([requests...], ordered=True)`, а не набором независимых конкурентных `client(request)`. Telegram получает pipeline сразу, но исполняет payment RPC последовательно.
- `MSG_WAIT_FAILED` и `MSG_WAIT_TIMEOUT` считаются ошибками dependency-wrapper: повторно pipeline-ится только request, который сервер ещё не исполнил.
- `FORM_SUBMIT_DUPLICATE`, сетевой сбой с неоднозначным состоянием и прочие результаты, где payment мог быть выполнен, автоматически не повторяются. Они проверяются и при необходимости оставляют payment hold.
- `gift-hunter-v0035-payment-audit.jsonl` пишет `fast_payment_batch_started`, `fast_payment_batch_phase`, `fast_payment_batch_finished`, binding-поля, возраст формы, send-start и итог каждого экземпляра.
- В batch audit пишется не секретный snapshot MTProto: `client_epoch`, `connect_epoch`, `connected`, `dc_id`, возраст клиента/соединения и `sender_reconnecting`.
- Между созданием локальной batch-task и prebuilt UDP FIRE по-прежнему нет await/log/disk I/O.

## Разбор production-сбоя v0034

Обе формы перед выстрелом были свежими (~72–73 с). Два payment RPC стартовали через `1.769` и `1.857` мс после trigger; один вернул `FORM_SUBMIT_DUPLICATE`, второй подтвердил `#444665`. Поэтому этот hotfix меняет именно механизм multi-submit, а не интервалы refresh.

Уведомление Telegram о новом устройстве содержит server timestamp `02:48:18 UTC`. В этот период лог действительно показывает первоначальные connect/auth действия MTProto. Выстрел произошёл около `11:01:22 UTC`; рядом с ним нет connect/disconnect/reconnect строк, то есть в том выстреле сессия не отваливалась.

Offline validation: `198` tests `OK`; живой Stars-залп в тестовой среде не выполнялся.
