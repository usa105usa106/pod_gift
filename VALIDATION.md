# Validation — Gift Hunter v0029

Проверка выполнена 5 августа 2026 года в отдельной Python 3.13 virtualenv. После создания релизного ZIP архив дополнительно распакован в новую директорию и повторно проверен из второй чистой virtualenv.

## Автоматические проверки

- `compileall` и AST-разбор всех Python-файлов;
- YAML-разбор `docker-compose.yaml`;
- ровно 6 сервисов `hunter-1` … `hunter-6`;
- 12 persistent volumes: шесть data-volume, один общий provision-volume и пять изолированных token-volume;
- отсутствие опубликованных наружу портов;
- отсутствие session/token-файлов в релизе;
- отсутствие ссылок на предыдущую версию;
- полный набор `unittest` с coverage;
- 100 000 случайных бинарных пакетов через пять UDP-декодеров;
- отдельные source-guards горячего пути;
- SHA-256 и release manifest готового архива.

```text
Ran 167 tests
OK
Coverage total: 61%
cluster.py: 83%
logic.py: 92%
main.py: 57%
```

## Критические исправления v0029

### FIRE привязан к коллекции

Подписанный пакет содержит:

```text
generation + campaign_id + shot + trigger
```

Проверено:

- FIRE другой коллекции с тем же номером игнорируется;
- FIRE другой generation игнорируется;
- неверный `trigger`, не равный `shot - 1`, не отправляется;
- пакет создаётся и подписывается при вооружении, а не в горячем пути;
- без pre-arm отправка возвращает ноль и не строит HMAC на лету.

### Первый обнаруживший не блокирует остальных

Проверен сценарий, когда первый Hunter видит точный предшественник, но его локальная форма отсутствует или устарела. Собственный платёж не стартует, однако заранее подготовленный UDP FIRE всё равно передаётся остальным участникам и фиксируется событием `fast_local_preflight_failed_relayed`.

### Реальная готовность UDP

Перед LIVE/Start в составе больше одного Hunter выполняется одноразовый подписанный PING/PONG. Docker DNS сам по себе больше не считается достаточной готовностью: каждый выбранный процесс должен ответить. Фонового постоянного Ping нет.

### Монотонная generation

Проверено восстановление поколения после потери `cluster.json` из:

- `cluster-generation.json`;
- lifecycle-файлов Hunter;
- последней корректной конфигурации в памяти.

Новая generation всегда больше максимального известного значения.

### Защита скорости

Статически и тестами подтверждено:

```text
создание всех локальных asyncio.Task
→ без await / logger / record_cluster_event / store.save
→ broadcast prebuilt UDP FIRE
```

В `broadcast_fire_nowait` отсутствуют `encode_fire` и HMAC-вычисление. FIRE/STATUS/config-notice callbacks не выполняют синхронную запись shared-volume в event loop.

Фактический send-start каждого платежа измеряется непосредственно перед:

```python
await client(prepared.request)
```

а не в момент создания задачи.

### Изоляция токенов

Hunter 2–6 используют пять отдельных read-only token-volume. Каждый вторичный контейнер видит только свой файл; Hunter 1 монтирует все пять для provisioning. Docker socket и публичный UDP-порт отсутствуют.

## Сохранённые функции

- Hunter 1: независимый залп `1–15`;
- Hunter 2–6: независимый залп `1–3` у каждого;
- точный predecessor-trigger без второго target lookup;
- локальные платёжные задачи раньше UDP FIRE;
- no app retry, post-submit verification и hold неопределённого результата;
- тихий режим, полная карточка и ручное RAM-обновление;
- ручной PONG с RAM, uptime и RTT стрелков;
- динамический каталог коллекций с current/total;
- безопасное изменение состава и watchdog;
- `/log_full` с `cluster.json`, `cluster-generation.json`, lifecycle и общим журналом.

## Ограничения среды

Автотесты используют проектные заглушки aiogram/Telethon. Production-зависимости из `requirements.txt` в проверочную virtualenv не устанавливались.

Не выполнялись:

- живой deployment в Coolify;
- реальный Bot API polling шести ботов;
- авторизация шести Telegram-аккаунтов;
- межконтейнерный UDP на реальном Docker bridge VPS;
- реальный `sendStarsForm`, списание Stars и получение Durov’s Glasses #4444.

Поэтому локальная логика, упаковка и горячий путь прошли автоматическую проверку, но точный номер Telegram не гарантируется и боевой Stars-путь окончательно подтверждается только на VPS.
