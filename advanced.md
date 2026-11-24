# shampoore: техническая документация

Здесь то, что не влезло в основной README: флаги запуска, ручная сборка, внутренности и подробности по Android SDK. Если вы просто хотите поставить и запустить, вам в [README](README.md).

---

## Сборка из исходников

Требования: Go 1.23+, `ffmpeg` (нужен для транспортов Telemost).

```bash
git clone https://github.com/Esteviredzu/shampoore.git
cd shampoore

# Сборка всех бинарников разом
go build -o bin/ ./cmd/...

# Запуск сервера в CLI-режимеz
./bin/shampoore-server -c config.json
```

```bash
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o bin/shampoore-panel ./cmd/shampoore-panel
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o bin/shampoore-server ./cmd/shampoore-server
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o bin/shampoore-relay ./cmd/shampoore-relay
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o bin/shampoore-node-agent ./cmd/shampoore-node-agent
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o bin/shampoore-client ./cmd/shampoore-client
```

Для ARM-серверов замените `GOARCH=amd64` на `GOARCH=arm64`.

После сборки установка запускается так:

```bash
sudo bash deploy/systemd/install.sh
```

Скрипт поставит бинарники в `/opt/shampoore/bin`, создаст systemd-сервис и сгенерирует токен доступа. Если рядом с ним нет готовых бинарников, он скачает их из последнего релиза. Чтобы установщик взял именно локально собранные файлы, положите их в `deploy/systemd/` рядом с `install.sh` и `shampoore.service`.

---

## Ручной запуск панели

Если запускать панель без systemd, основные флаги такие:

```bash
shampoore-panel \
  --listen=127.0.0.1:8080 \
  --public-listen=127.0.0.1:8443 \
  --db=/var/lib/shampoore/panel.db \
  --public-url=https://sub.example.com
```

- `--listen` - адрес админки. По умолчанию `127.0.0.1:8080`.
- `--public-listen` - адрес публичного API. По умолчанию `127.0.0.1:8443`, значение `off` отключает его.
- `--db` - путь к базе SQLite.
- `--public-url` - внешний адрес панели и подписок.
- `--cleanup-all-nodes` - служебный режим, чистит все ноды перед удалением.
- `--allow-public-admin` - разрешает слушать админку не на loopback. Лучше так не делать и пользоваться SSH-туннелем.

---

## Флаги сервера

У `shampoore-server` для GeoIP и цепочки Exit есть отдельные флаги. Для обычного запуска достаточно `-c config.json`, но иногда пригождается:

- `--rules-dir` - папка с GeoIP-правилами.
- `--block-unknown` - блокировать трафик, для которого не нашлось правило.
- `--socks-listen` - необязательный локальный SOCKS5; по умолчанию выключен, адрес принудительно приводится к loopback (`127.0.0.1`).
- `--exit-upstream` - адрес вышестоящего SOCKS5 для ручной цепочки; панель больше не генерирует этот флаг при деплое exit-нод.

---

## Удаление подробнее

```bash
sudo /opt/shampoore/uninstall.sh --purge
```

`--purge` удаляет бинарники, systemd-сервис, конфигурации, состояние и системного пользователя. Если запустить без флага, конфиги останутся на месте, и можно будет поставить всё заново поверх.

---

## Внутренности

- Транспорты регистрируются через `app.Descriptor`, а `app.BuildRuntime` собирает единый конвейер: `транспорт → SecurePipe → [агрегатор flowmux / bond] → multiplexer`.
- Надёжность доставки обеспечивается на уровне каждого транспорта индивидуально:
  - **`yandex_telemost_*`** — **KCP** поверх UDP/RTP медиапотока;
  - **`yandex` (Яндекс Доска)** — встроенная **reliable session** с нумерацией и повторами;
  - **`raw_ws`** — полагается на **TCP** (TLS-in-TLS) + подтверждение записи батчей в сокет;
  - **`livekit_datachannel`** — надёжный режим **WebRTC DataChannel (SCTP)**;
  - **`silo`** — упорядоченные S3-объекты с лексикографической сортировкой ключей.

---

## Android SDK подробнее

Библиотека собирается через `gomobile bind`:

```bash
./build_aar.sh
```

На выходе `shampoore.aar`. Кладёте в `app/libs/`, подключаете:

```kotlin
// app/build.gradle.kts
implementation(files("libs/shampoore.aar"))
```

Методы, которые обычно нужны:

```kotlin
import mobile.Mobile

// Запуск в режиме VPN, передав файловый дескриптор TUN
val err = Mobile.startClientVPN(configUrl, tunFd, 1500)
if (err.isNotEmpty()) {
    // Обработка ошибки
}

// Запуск без VPN, в режиме SOCKS5
val err2 = Mobile.startClient(configUrl, "127.0.0.1:1080")

// Статистика и состояние
val statsJson = Mobile.getStats()
val state = Mobile.getState()

// Проверка здоровья соединения
val health = Mobile.getHealth()
val connected = Mobile.isConnected()

// Остановка
Mobile.stopClient()
```

Для Android VPN нужно ещё прокинуть `SocketProtector`, чтобы сокеты не уходили в туннель:

```kotlin
Mobile.setSocketProtector { fd ->
    vpnService.protect(fd)
}
```

При смене сети (Wi-Fi на LTE и наоборот) полезно дёрнуть:

```kotlin
Mobile.notifyNetworkChanged()
```

---

## Релиз

Сборка релизов в `dist/`:

```bash
./release.sh [version]
```