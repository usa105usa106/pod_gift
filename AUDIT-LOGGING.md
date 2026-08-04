# Аудит логирования v0029

## FAST-снайпер и залп

При запуске каждого Hunter:

```text
scanner_started version=v0029 shooter_id=... mode=sniper|volley active_shooters=...
saved_ids=... targets=... live=... volley=... volley_limit=...
```

После отправки залпа:

```text
fast_volley_completed shooter_id=... source=local|udp campaign_id=...
slug=... predecessor=4443 target=4444 volley=... volley_limit=...
udp_peers_sent=... first_send_start_ms=... task_launch_ms=... outcomes=[...]
```

В `outcomes` для каждого `saved_id` сохраняются:

- `status`;
- фактически полученный `actual_num`;
- `send_start_ms` — время от обнаружения триггера до входа именно этого запроса в MTProto-вызов;
- безопасно обрезанное описание ошибки.

`campaign_id` привязывает сигнал к конкретной коллекции. FIRE с другой коллекцией, другой generation или другим номером не запускает залп.

Если первый обнаруживший не может отправить собственный платёжный план, но передал сигнал остальным, пишется:

```text
fast_local_preflight_failed_relayed shooter_id=... source=local campaign_id=...
slug=... predecessor=4443 target=4444 volley=... udp_peers_sent=... error=...
```

Это позволяет отличить «первый увидел, сам не выстрелил, но разбудил остальных» от полного отсутствия триггера.

## UDP и готовность состава

Перед LIVE/Start при составе больше одного выполняется разовый подписанный UDP-опрос:

```text
cluster_preflight_ping generation=... active_shooters=... results=... missing=...
```

Постоянного фонового Ping нет. Ручная кнопка пишет:

- `ping_started` — RAM, лимит RAM, uptime и скрытые диагностические показатели сканера;
- `ping_check_started`;
- `peer_pong` — shooter_id и RTT;
- `peer_timeout`;
- `ping_check_completed`;
- `ping_completed`.

Hunter 1 принимает служебный статус участника только при совпадении generation и campaign. Отклонение фиксируется:

```text
cluster_status_rejected shooter_id=... reason=generation_mismatch|campaign_mismatch
status_generation=... active_generation=... status_campaign=... expected_campaign=...
```

## Изменение состава и восстановление generation

Ключевые события:

- `cluster_reconfigure_requested`;
- `cluster_configured`;
- `cluster_config_notice_sent` / `cluster_config_notice_received`;
- `cluster_config_notice_quiesce`;
- `cluster_config_invalid_ignored`;
- `cluster_config_applied`;
- `cluster_participant_deactivation_started` / `cluster_participant_deactivated`;
- `cluster_reconfigure_confirmed`;
- `watchdog_stall`, `cluster_runtime_failed`, `application_failed`.

Монотонное поколение дополнительно хранится в `/provision/cluster-generation.json`. При потере `cluster.json` Hunter 1 восстанавливает максимум generation из marker и lifecycle-файлов, затем создаёт следующее значение.

UDP callbacks не пишут shared-volume синхронно в event loop. STATUS и config-notice сначала обрабатываются в RAM, а журнал записывается через `asyncio.to_thread`.

## Подключение Hunter 2–6

Hunter 1 пишет:

- `shooter_token_received`;
- `shooter_token_message_delete_failed`;
- `shooter_token_duplicate_rejected`;
- `shooter_token_validation_failed`;
- `shooter_token_provisioned`;
- `cluster_token_setup_complete`.

Значение токена не записывается. Hunter 2–6 используют пять отдельных token-volumes; каждый вторичный контейнер видит только собственный файл.

## Каталог подарков

Кнопка «Проверить номера» пишет начало, результат каждой коллекции, подсветку красивого номера и итог формирования. Полный снимок находится в `catalog-numbers-latest.json` и входит в `/log_full`.

## `/log_full`

ZIP текущего Hunter включает:

- обычные и ротированные логи его data-volume;
- diagnostics, каталог, stress и rate-limit;
- `cluster.json`;
- `cluster-generation.json`;
- общий `cluster-events.jsonl`;
- lifecycle-файлы Hunter 1–6.

По архиву Hunter 1 видны ключевые переходы всей группы. Низкоуровневый процессный лог конкретного участника выгружается командой `/log_full` в его собственном боте.

Поля с именами `token`, `secret`, `password`, `api_hash` в общем журнале автоматически заменяются на `[redacted]`.
