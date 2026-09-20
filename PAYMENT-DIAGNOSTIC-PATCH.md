# Payment diagnostics and FAST volley — v0038

v0038 hardens multi-payment Stars volleys after the production `#444665` run.

## Что изменено

- Формы живут по прежней политике `300 с` дальше 50 номеров и `120 с` внутри 50; на входе в near-target зону весь комплект refresh-ится принудительно.
- Каждый paid plan проверяется по `saved_id`, invoice binding, `form_id`, request binding и возрасту формы. Повторяющийся `form_id` блокирует залп fail-closed.
- После живого v0036 теста полный simultaneous submit заменён на micro-stagger: каждый `SendStarsFormRequest` ставится напрямую в `client._sender` как `ordered=False`, но соседние queue-start разделены на `10` мс по умолчанию. Ответ предыдущего платежа не ожидается.
- Настройка `fast_volley_stagger_ms` сохраняется в `settings.json`; `/stagger 10` меняет её, `/settings` и `/help` показывают управление.
- `invokeAfterMsg`/ordered dependency не используются, поэтому старой задержки порядка `521.728` мс нет.
- Финансовый submit не использует client-level request retry. Неоднозначный результат не вызывает автоматический повтор платежа.
- `gift-hunter-v0038-payment-audit.jsonl` пишет `fast_payment_batch_started`, `fast_payment_batch_dispatched`, `fast_payment_batch_finished`, `stagger_ms`, реальные `queue_offsets_ms`, binding-поля, возраст формы, send-start и итог каждого экземпляра.
- Между созданием локальной batch-task и prebuilt UDP FIRE по-прежнему нет await/log/disk I/O.

## Разбор production-сбоя v0034

Обе формы перед выстрелом были свежими (~72–73 с). Два payment RPC стартовали через `1.769` и `1.857` мс после trigger; один вернул `FORM_SUBMIT_DUPLICATE`, второй подтвердил `#444665`. Поэтому этот hotfix меняет именно механизм multi-submit, а не интервалы refresh.

Уведомление Telegram о новом устройстве содержит server timestamp `02:48:18 UTC`. В этот период лог действительно показывает первоначальные connect/auth действия MTProto. Выстрел произошёл около `11:01:22 UTC`; рядом с ним нет connect/disconnect/reconnect строк, то есть в том выстреле сессия не отваливалась.

Offline validation: `198` tests `OK`; живой Stars-залп в тестовой среде не выполнялся.
