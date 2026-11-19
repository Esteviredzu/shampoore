<p align="center">
  <img src="logo.jpg" alt="logo" width="300">
</p>

<h1 align="center">shampoore</h1>

<p align="center">
  <em>Модульная платформа для прокси-туннелирования поверх кастомных сервисов.</em>
</p>

<p align="center">
  <a href="https://github.com/Esteviredzu/shampoore/releases"><img src="https://img.shields.io/github/v/release/Esteviredzu/shampoore?style=flat-square" alt="Release"></a>
  <a href="https://go.dev/"><img src="https://img.shields.io/badge/Go-1.23+-00ADD8?style=flat-square&logo=go" alt="Go Version"></a>
  <a href="https://github.com/Esteviredzu/shampoore/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-AGPL--v3-green?style=flat-square" alt="License"></a>
</p>

> **Дисклеймер:** проект предоставляется «как есть» для исследовательских и образовательных целей. Автор не несёт ответственности за использование в нарушение законодательства или условий сторонних сервисов. Используя код, вы берёте ответственность на себя.

---

## Обзор

**shampoore** - это исследовательский инструмент для инкапсуляции трафика в запросы к публичным API.

В состав входят: `shampoore-server` (демон), `shampoore-panel` (веб-панель), `shampoore-relay` (входной сервер), `shampoore-node-agent` (агент выходной ноды), `shampoore-client` (CLI, SOCKS5/TUN) и Mobile SDK (AAR для Android).

Сейчас проект умеет работать не только как одиночный сервер, но и как мультипользовательский каскад с Relay и Exit. Об этом ниже.

## Возможности

- **Транспорты**: `yandex`, `yandex_telemost_raw`, `yandex_telemost_squares`, `livekit_datachannel`, `silo`, `multi` (агрегация через `flowmux` или `bond`).
- **Мультипользовательский режим**: Relay и Exit, общие комнаты для пользователей, учет трафика и лимитов.
- **GeoIP**: блокировка трафика по спискам CIDR/IP на выходных серверах.
- **Безопасность**: XChaCha20-Poly1305.
- **Панель**: Material 3 дизайн, управление конфигами, QR-экспорт, мониторинг CPU/RAM, подписки и деплой нод.
- **Клиент**: локальный SOCKS5 или полноценный TUN.

## Архитектура

В простом виде схема выглядит так:

```text
+---------------------------+
| Клиент (SOCKS5 или TUN)   |
+-------------+-------------+
              |
              v
+---------------------------+
| Relay (вход)              |
| LiveKit, Raw WebSocket    |
| Silo S3                   |
+-------------+-------------+
              |
              v
+---------------------------+
| Exit (выход)              |
| учет трафика, лимиты      |
| GeoIP-правила             |
+-------------+-------------+
              |
              v
+---------------------------+
| Интернет                  |
+---------------------------+

+---------------------------+
| shampoore-panel           |
| админка: 127.0.0.1:8080   |
| публично: 127.0.0.1:8443  |
| SSH, SQLite, подписки     |
+---------------------------+
```

Панель управляет Relay и Exit по SSH, хранит состояние в SQLite и отдаёт подписки через отдельный публичный API. Relay принимает клиентов, Exit выпускает трафик в интернет, а клиент может ходить как через SOCKS5, так и через TUN.

## Мультипользовательский каскад

Одна из главных новых частей проекта - каскад из входных и выходных серверов. Клиент подключается к Relay, Relay передает трафик на Exit, а Exit выпускает его в интернет.

- **Входные серверы (Relay)** принимают клиентов через общие транспорты: `LiveKit`, `Raw WebSocket` и `Silo S3`. Пользователи не пересекаются: у каждого своя комната `route-<id>-<user_id>` в LiveKit и Raw WebSocket, а в Silo (образ `docker.io/pgsty/silo`) данные лежат в отдельном префиксе `users/<user_id>/`.
- **Выходные серверы (Exit)** считают трафик по каждому пользователю, проверяют лимиты и срок подписки, а также блокируют трафик по GeoIP-правилам на своей стороне.
- **Агент ноды** (`shampoore-node-agent`) раз в 30 секунд отправляет дельты трафика и обновляет список активных пользователей на Exit.

Так один Relay может обслуживать много пользователей, а Exit следит, чтобы у всех всё было в рамках лимитов.

## Веб-панель и безопасность

`shampoore-panel` - это центр управления проектом. Панель я старался сделать удобной, но безопасной по умолчанию.

- Админка по умолчанию слушает только `127.0.0.1:8080`, поэтому снаружи её не видно. Заходить нужно через SSH-туннель: `ssh -L 8080:127.0.0.1:8080 user@server`.
- Публичный порт `127.0.0.1:8443`, который прикрывает Caddy, отдаёт только подписки `GET /sub/{token}` и синхронизацию нод `POST /api/node/sync`. Остальное туда не попадает.
- Все данные (ноды, маршруты, пользователи и настройки) лежат в SQLite. Используется `modernc.org/sqlite`, поэтому CGO не нужен.
- HTTPS для домена подписок настраивается прямо в интерфейсе панели: она сама готовит конфиг для Caddy. Там же есть переключатель, который включает или выключает сетевые подписки.
- Для пользователя можно выдать оффлайн JSON-бандл со ссылками `shampoore://` и QR-кодом.
- Ноду можно задеплоить одной кнопкой по SSH: панель поставит Docker, загрузит конфиги и бинарники, пересоздаст контейнеры через `--force-recreate` и сама создаст бакет для Silo.
- SSH-деплой по умолчанию не проверяет host key. Для production лучше задать свой `ssh.HostKeyCallback` или работать в доверенной сети.
- После изменения маршрута панель сама переразворачивает затронутую Exit-ноду.
- При удалении ноды панель полностью её зачищает (`CleanupNode`), а `uninstall.sh --purge` убирает локальную установку.

## GeoIP-правила

На выходных серверах можно блокировать трафик по географии. Правила добавляются двумя способами:

- вручную - вставить список CIDR/IP;
- по ссылке - панель скачает список и будет обновлять его с заданным интервалом. Ссылки GitHub вида `blob` она сама превращает в `raw`.

Ещё правила можно привязать галочками к конкретным Exit-серверам, чтобы на разных нодах работали разные списки.

Готовая ссылка для российских IPv4-подсетей:

```text
https://github.com/ipverse/country-ip-blocks/blob/master/country/ru/ipv4-aggregated.txt
```

## Установка

Поддерживаются системы на базе Linux с systemd. Для установки сервера и панели одной командой выполните:

```bash
curl -fsSL https://github.com/Esteviredzu/shampoore/releases/latest/download/install.sh | sudo bash
```

Скрипт автоматически:

1. Создаст системного пользователя `shampoore`.
2. Установит бинарники в `/opt/shampoore/bin`.
3. Сгенерирует токен доступа и настроит `/etc/shampoore/panel.env`.
4. Запустит systemd-сервис `shampoore.service` с автозапуском при перезагрузке.

### Доступ к панели

После установки панель слушает только `127.0.0.1:8080`. Как открыть её через SSH-туннель, написано в разделе «Веб-панель и безопасность».

### Удаление

```bash
sudo /opt/shampoore/uninstall.sh --purge
```

Флаг `--purge` удаляет бинарники, systemd-сервис, конфигурации, состояние и системного пользователя. Перед удалением `panel.db` скрипт пытается очистить все ноды из базы через SSH (`--cleanup-all-nodes`, best effort: недоступная VPS не блокирует purge). Если вы хотите сохранить конфигурации, запустите скрипт без флага: `sudo /opt/shampoore/uninstall.sh`.

## Быстрая сборка и установка

Если нужно собрать всё самому, удобно сразу сделать бинарники под Linux. `CGO_ENABLED=0` нужен, чтобы не тянуть системные библиотеки:

```bash
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o bin/shampoore-panel ./cmd/shampoore-panel
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o bin/shampoore-server ./cmd/shampoore-server
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o bin/shampoore-relay ./cmd/shampoore-relay
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o bin/shampoore-node-agent ./cmd/shampoore-node-agent
```

Для ARM-серверов замените `GOARCH=amd64` на `GOARCH=arm64`.

После сборки установка запускается так:

```bash
sudo bash deploy/systemd/install.sh
```

Скрипт поставит бинарники в `/opt/shampoore/bin`, создаст systemd-сервис и сгенерирует токен доступа. Если рядом с ним нет готовых бинарников, он скачает их из последнего релиза. Чтобы установщик взял именно локально собранные файлы, положите их в `deploy/systemd/` рядом с `install.sh` и `shampoore.service`.

### Ручной запуск панели

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
- `--cleanup-all-nodes` - служебный режим очистки всех нод перед удалением.
- `--allow-public-admin` - разрешает слушать админку не на loopback. Лучше так не делать и пользоваться SSH-туннелем.

У `shampoore-server` для GeoIP и цепочки Exit есть `--rules-dir`, `--block-unknown`, `--socks-listen` и `--exit-upstream`. Для обычного запуска достаточно `-c config.json`.

## Использование клиента

Клиент может принимать JSON-файл или URL формата `shampoore://`. Конфиг создаётся в панели (вкладка Configs) или экспортируется оттуда как `shampoore://`-ссылка.

```bash
# Режим SOCKS5-прокси (по умолчанию на 127.0.0.1:1080)
shampoore-client -c config.json -m socks5

# Режим VPN (TUN)
sudo shampoore-client -c shampoore://... -m tun
```

## Сборка из исходников

Требования: Go 1.23+, `ffmpeg` (для транспортов Telemost).

```bash
git clone https://github.com/Esteviredzu/shampoore.git
cd shampoore

# Сборка бинарников
go build -o bin/ ./cmd/...

# Запуск сервера (CLI-режим)
./bin/shampoore-server -c config.json
```

## Android SDK

Для мобильных приложений доступна скомпилированная библиотека AAR. Скачайте `shampoore.aar` из раздела Releases и положите в `app/libs/`.

```kotlin
// app/build.gradle.kts
implementation(files("libs/shampoore.aar"))
```

Пример инициализации в Kotlin:

```kotlin
import mobile.Mobile

// Запуск в режиме VPN (передав файловый дескриптор TUN)
val err = Mobile.startClientVPN(configUrl, tunFd, 1500)
if (err.isNotEmpty()) {
    // Обработка ошибки
}

// Получение статистики
val statsJson = Mobile.getStats()
```

---

<sub> Я вдохновился [olcrtc](https://github.com/openlibrecommunity/olcrtc) и [OpenFlux](https://github.com/p1neapplexpress/OpenFlux) и захотел наколхозить что-то своё.</sub>
