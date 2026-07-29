# Gift Hunter v0002

Упрощённая версия без папок проекта и без тестовых файлов. Для GitHub нужны все файлы из этого архива:

- `main.py` — весь код бота в одном файле;
- `Dockerfile` — сборка и запуск в Coolify;
- `requirements.txt` — зависимости Python;
- `.env.example` — пример переменных;
- `.gitignore` — защита секретов и session-файлов;
- `README.md` — инструкция.

Папку `data` в GitHub создавать не нужно. Приложение создаёт `/app/data` само; в Coolify подключи к этому пути Persistent Storage.

## Coolify

1. Загрузи все шесть файлов в корень GitHub-репозитория.
2. В Coolify создай Application из этого репозитория, Build Pack: Dockerfile.
3. Добавь Persistent Storage с Destination path `/app/data`.
4. Добавь переменные:

```env
BOT_TOKEN=токен_из_BotFather
SETUP_PIN=любой_секретный_PIN
MAX_UPGRADE_STARS=2000
SCAN_INTERVAL_MS=400
```

5. Deploy и отправь боту `/start`.
6. В чате введи `TG_API_ID`, `TG_API_HASH` и номер телефона.
7. Для одноразового кода открой Termius и выполни команду, которую покажет бот. Она будет вида:

```bash
docker exec -it ID_КОНТЕЙНЕРА python main.py auth
```

8. Введи код и пароль 2FA в Termius, затем в чате нажми `✅ Проверить авторизацию`.

Код входа нельзя отправлять сообщением в Telegram-чат: Telegram аннулирует такие коды.

## Кнопки

- `🎁 Подарки`
- `🔢 Проверить номера`
- `🎯 Задать номера`
- `▶️ Запустить` / `⛔ Остановить`
- `🛡 Оплата: ВЫКЛ` / `💳 Оплата: ВКЛ`
- `📡 Ping`
- `📄 Log`
- `🗑 Сброс`

Команды: `/start`, `/status`, `/log_full`.

После каждого перезапуска платный режим автоматически возвращается в `ВЫКЛ`.
