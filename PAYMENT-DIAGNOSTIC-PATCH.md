# Payment diagnostics and FAST volley — v0042 (NEXT-TICK + RESPONSE-CHAIN)

## Текущий механизм

- Все FAST payment forms готовятся и обновляются заранее по прежней политике `300 с / 120 с внутри 50`.
- Перед submit проверяются `saved_id`, invoice binding, `form_id`, request binding и возраст формы; повторяющийся `form_id` блокирует залп.
- Durable payment guard удалён полностью из firing path. Никакого `fsync` перед первым Stars-submit нет.
- Submit идёт напрямую через уже подключённый `client._sender`, `ordered=False`, без client-level retry.
- `/otvet off` (default): первый request ставится сразу, перед следующим выполняется один `await asyncio.sleep(0)`; ответ Telegram не ожидается. `/otvet on`: следующий request ставится сразу после завершения raw RPC future предыдущего, без промежуточной verification/UI/disk работы.
- Нет фиксированного micro-stagger и нет `invokeAfterMsg`/ordered dependency.
- Неоднозначный результат автоматически не повторяется.
- `gift-hunter-v0042-payment-audit.jsonl` пишет `dispatch_mode=sequential_next_tick|sequential_rpc_response`, `waits_for_previous_response`, `queue_offsets_ms`, `queue_deltas_ms`, `response_offsets_ms`, binding-поля, возраст формы и итог каждого экземпляра.

## Зачем это сделано

Полностью simultaneous submit давал `FORM_SUBMIT_DUPLICATE`. Фиксированный 10 мс stagger в живом v0039 тесте фактически дал около `12.418` мс и всё равно не устранил конфликт. v0042 позволяет проверить оба оставшихся подхода без новой сборки: NEXT-TICK (`/otvet off`) и RESPONSE-CHAIN (`/otvet on`).

Offline validation: `199` tests `OK`; живой Stars-залп в тестовой среде не выполнялся.
