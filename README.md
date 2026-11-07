<p align="center">
  <img src="logo.jpg" alt="logo" width="300">
</p>

<h1 align="center">shampoore-core</h1>

<p align="center">
  <em>Ядро модульного прокси-туннеля поверх кастомных сервисов.</em>
</p>

---

## Описание

**shampoore-core** - ядро для организации сетевого туннеля через публичные сервисы, доступ к которым не ограничен.

## Поддерживаемые платформы

- **Linux**
- **Android**

## Существующие транспорты

- **LiveKit DataChannel**
- **Yandex Board**
- **Yandex Telemost raw**
- **Yandex Telemost squares**

## Установка

```bash
git clone https://github.com/Esteveredzu/shampoore-core.git
cd shampoore-core
go mod download
```

## Сборка

### Linux

```bash
go build -o bin/ ./cmd/...
```

В `bin/` появятся три бинарника: `shampoore-client`, `shampoore-server` и `shampoore-panel`.

### Android

Требования: Go >= 1.22, Android NDK (рекомендуется 25+), gomobile.

Установите gomobile один раз:

```bash
go install golang.org/x/mobile/cmd/gomobile@latest
go install golang.org/x/mobile/cmd/gobind@latest
gomobile init
```

```bash
export ANDROID_NDK_HOME=/path/to/ndk
./build-aar.sh
```

На выходе в корне проекта появятся:

- `shampoore.aar`
- `shampoore-sources.jar`

В директории **`examples/`** лежат готовые примеры конфигов. Свои конфиги складывайте в **`configs/`**.

## Где брать хэши для Yandex-транспортов

### Yandex Board

1. Откройте [boards.yandex.ru](https://boards.yandex.ru) и создайте новую доску.
2. Настройте доступ: **Поделиться** -> **Редактирование для всех** -> **Включить гостевой доступ** -> **Права гостей: редактирование**.
3. В ссылке будет хэш `https://boards.yandex.ru/whiteboard/?hash=YOUR_HASH`.

### Yandex Telemost

1. Откройте [telemost.yandex.ru](https://telemost.yandex.ru) и создайте конференцию.
2. Хэш будет в ссылке `https://telemost.yandex.ru/j/YOUR_HASH`.

Клиент и сервер должны находиться в **одном** звонке, каждый заходит отдельным участником.

## Свой LiveKit мост на VPS

LiveKit - это SFU, к которому подключаются и клиент, и сервер как два участника одной комнаты. Между собой они напрямую не соединяются, весь трафик идёт через ваш VPS.

Ниже готовый скрипт для VPS с Ubuntu/Debian. Он ставит Docker, генерирует секрет, поднимает LiveKit и Caddy (автоматический TLS от Let's Encrypt). Домен должен уже указывать A-записью на IP вашего VPS.

`LK_KEY` и `LK_SECRET` - это пара ключей для аутентификации в LiveKit. `LK_KEY` вы придумываете сами, `LK_SECRET` генерируется автоматически. Оба значения пойдут в конфиг shampoore, их нужно скопировать из вывода скрипта в конце.

```bash
# на VPS, от root или через sudo -s

export LK_DOMAIN="call.example.com"                     # ваш домен
export LK_KEY="my-livekit-key"                          # придумайте любой осмысленный
export LK_SECRET="$(openssl rand -base64 32)"           # сгенерируется сам

mkdir -p ~/livekit/deploy && cd ~/livekit

cat > deploy/livekit.yaml <<EOF
port: 7880
rtc:
  tcp_port: 7881
  port_range_start: 50000
  port_range_end: 50100
  use_external_ip: false
keys:
  ${LK_KEY}: ${LK_SECRET}
logging:
  level: info
EOF

cat > deploy/Caddyfile <<EOF
${LK_DOMAIN} {
    reverse_proxy livekit:7880
}
EOF

cat > docker-compose.yml <<'EOF'
version: "3.8"

services:
  livekit:
    image: livekit/livekit-server:latest
    container_name: shampoore-livekit
    restart: unless-stopped
    command: --config /etc/livekit.yaml
    ports:
      - "7881:7881"
      - "50000-50100:50000-50100/udp"
    volumes:
      - ./deploy/livekit.yaml:/etc/livekit.yaml:ro
    networks:
      - shampoore-net

  caddy:
    image: caddy:2-alpine
    container_name: shampoore-caddy
    restart: unless-stopped
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - ./deploy/Caddyfile:/etc/caddy/Caddyfile:ro
      - caddy-data:/data
      - caddy-config:/config
    depends_on:
      - livekit
    networks:
      - shampoore-net

networks:
  shampoore-net:
    driver: bridge

volumes:
  caddy-data:
  caddy-config:
EOF

ufw allow 80/tcp
ufw allow 443/tcp
ufw allow 7881/tcp
ufw allow 50000:50100/udp

docker compose up -d

echo "===== СОХРАНИТЕ ЭТИ ЗНАЧЕНИЯ ====="
echo "api_key=${LK_KEY}"
echo "api_secret=${LK_SECRET}"
echo "==================================="
```

Проверьте, что сертификат выпустился и LiveKit живой:

```bash
curl -I https://${LK_DOMAIN}
# должен вернуть HTTP/2 404
```

Дальше подставьте в `configs/livekit.json` значения из вывода скрипта. `api_key` - это `LK_KEY`, `api_secret` - это `LK_SECRET`:

```json
{
  "name": "my-livekit",
  "type": "livekit_datachannel",
  "encryption_password": "your_password",
  "transport": {
    "signal_url": "wss://call.example.com",
    "api_key": "my-livekit-key",
    "api_secret": "LK_SECRET",
    "room": "shampoore-room-1",
    "data_topic": "shampoore"
  }
}
```

В `signal_url` подставьте свой домен, в `api_key` - свой `LK_KEY` (тот, что вы придумали в скрипте), в `api_secret` - распечатанный `LK_SECRET`. Поле `encryption_password` придумайте сами, оно должно совпадать на клиенте и сервере. Тот же файл положите на сервер и на клиент.

## Запуск

### Режим с веб-панелью

Запустите `shampoore-panel` и откройте `http://127.0.0.1:8080`. Панель сама поднимет `shampoore-server` и будет управлять конфигами.

```bash
./bin/shampoore-panel -listen 127.0.0.1:8080 -configs-dir ./configs -examples ./examples
```

### CLI

Вариант 1 - перечислить файлы:

```bash
./bin/shampoore-server -c config1.json -c config2.json
```

Вариант 2 - указать директорию с конфигами:

```bash
./bin/shampoore-server --configs-dir ./configs
```

Тогда сервер возьмёт все `*.json` из директории и запустит все сразу.

### Запуск клиента

## Linux

```bash
# SOCKS5
./bin/shampoore-client -c config.json -m socks5

# TUN
sudo ./bin/shampoore-client -c config.json -m tun
```

Проверка:

```bash
curl -4 --socks5-hostname 127.0.0.1:1080 https://ifconfig.me
curl -4 https://ifconfig.me
```

## Ссылки shampoore://

Любой конфиг можно превратить в URL и потом также запускать с флагом `-c`:

```bash
./bin/shampoore-server -encode -c configs/livekit.json
./bin/shampoore-server -c "shampoore://...."
```

## О проекте

Я вдохновился [olcrtc](https://github.com/openlibrecommunity/olcrtc) и [OpenFlux](https://github.com/p1neapplexpress/OpenFlux) и захотел наколхозить что-то своё.