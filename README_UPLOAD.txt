ГОТОВЫЙ ФИКС ДЛЯ COOLIFY
========================

Почему падало
-------------
Старый Docker HEALTHCHECK запускал `python main.py healthcheck`. При каждом
запуске заново импортировались aiogram, Telethon и весь большой main.py.
Coolify давал проверке 5 секунд; импорт не успевал завершиться, поэтому
контейнер получал статус unhealthy и откатывался.

Что изменено
------------
1. Добавлен отдельный лёгкий healthcheck.py только на стандартной библиотеке.
2. Dockerfile запускает именно healthcheck.py и даёт боту 45 секунд на старт.
3. Сам Telegram-бот запускается как worker: `python -u /app/main.py bot`.
4. Добавлен .dockerignore, чтобы секреты, session-файлы и data не попадали в образ.

Что загрузить в GitHub
----------------------
В корень репозитория загрузи с заменой:
- Dockerfile
- healthcheck.py
- .dockerignore
- .env.example (необязательно, это только пример без настоящего токена)

Файлы main.py и requirements.txt оставь в репозитории как есть.

Coolify
-------
Build Pack: Dockerfile
Persistent Storage destination: /app/data

Environment Variables:
BOT_TOKEN=<токен BotFather>
SETUP_PIN=<твой секретный PIN>

Остальные переменные необязательны: в коде есть значения по умолчанию.
После загрузки файлов нажми Redeploy. Выключать Health Check в Coolify не надо.

Важно
-----
Настоящий BOT_TOKEN не записывай в .env.example и не загружай в GitHub.
