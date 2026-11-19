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

В состав входят: `shampoore-server` (демон), `shampoore-panel` (веб-панель), `shampoore-client` (CLI, SOCKS5/TUN) и Mobile SDK (AAR для Android).

## Возможности

- **Транспорты**: `yandex`, `yandex_telemost_raw`, `yandex_telemost_squares`, `livekit_datachannel`, `silo`, `multi` (агрегация через `flowmux` или `bond`).
- **Безопасность**: XChaCha20-Poly1305.
- **Панель**: Material 3 дизайн, управление конфигами, QR-экспорт, мониторинг CPU/RAM.
- **Клиент**: локальный SOCKS5 или полноценный TUN.

## Архитектура

```text
[ Клиент ] -> [ shampoore-client ] -> [ Кастомный транспорт ] -> [ shampoore-server ] -> [ Интернет ]
                                                                          ^
                                                                          | Unix Socket
                                                                  [ shampoore-panel ]
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

По умолчанию панель слушает `127.0.0.1:8080` и недоступна извне. Для безопасного доступа используйте SSH-туннель:

```bash
ssh -L 8080:127.0.0.1:8080 user@your-server
```

Оставьте это окно терминала открытым, затем откройте в браузере: `http://localhost:8080/?token=<ТОКЕН_ИЗ_ВЫВОДА_УСТАНОВЩИКА>`.

### Multiuser control plane (Relay + Exit)

Панель хранит ноды, маршруты, подписчиков и настройки в SQLite (`modernc.org/sqlite`, без CGO). Публичный API для подписок включён по умолчанию и слушает только loopback:

```bash
shampoore-panel \
  --listen=127.0.0.1:8080 \
  --db=/var/lib/shampoore/panel.db
```

- `127.0.0.1:8080` — админка, админские `/api/*`-ручки, токен + HttpOnly cookie. Доступ — через SSH-туннель.
- `127.0.0.1:8443` — только `GET /sub/{sub_token}` и `POST /api/node/sync`; статика и админка недоступны. Адрес можно изменить в настройках панели (`public_listen`).
- `--public-listen=off` полностью отключает публичный API. Явно заданный `--public-listen` имеет приоритет над настройками.
- `--public-url` — необязательный override. Если не указан, панель берёт адрес из настроек (`public_url` / `public_domain`), поэтому домен подписок можно менять прямо в UI без перезапуска systemd.
- `--cleanup-all-nodes` — служебный режим: открывает `--db`, проходит по всем нодам из SQLite и вызывает `Provisioner.CleanupNode` на каждой (`docker compose down -v` + удаление `/opt/shampoore` по SSH), затем завершается. Используется `uninstall.sh --purge` перед удалением `panel.db`.
- `PUT /api/settings` сохраняет домен подписок и автоматически пишет reverse-proxy в локальный Caddy (`/etc/caddy/conf.d/shampoore-sub.caddy` при наличии `import conf.d/*`, иначе — управляемый блок в `Caddyfile`) и перезагружает его.
- Панель пишет drop-in напрямую (`ReadWritePaths=-/etc/caddy` в systemd-юните) и перезагружает Caddy через `systemctl`, `sudo -n` или loopback admin API `127.0.0.1:2019`. Если Caddy не установлен, настройки сохраняются, а UI показывает предупреждение.
- `POST /api/nodes/{id}/deploy` запускает SSH-деплой Relay/Exit-ноды через Docker Compose (`up -d --force-recreate --remove-orphans`) и возвращает пошаговый лог. Для Silo бакет создаётся автоматически.
- `DELETE /api/nodes/{id}` сначала очищает сервер (`docker compose down -v --remove-orphans` + `rm -rf /opt/shampoore`). Если VPS уже удалён, используйте `?force=true`.
- Любое изменение маршрута автоматически переразворачивает затронутую Exit-ноду в фоне.
- Exit-нода запускает `shampoore-node-agent`: он раз в 30 секунд отправляет дельты трафика, получает активных пользователей и обновляет multiuser-конфиги через control socket.
- Режим LiveKit `on-demand` (`/session`, `token_url`, `token_listen`, `token_secret`) удалён. Все транспорты (`raw_ws`, `silo`, `livekit`) работают в едином multiuser-режиме: клиент сразу получает параметры своей изолированной комнаты (`route-<id>-<user_id>` / `users/<user_id>/`).
- Если панель и Relay установлены на одном сервере, Relay автоматически разворачивается без контейнерного Caddy: 80/443 остаются у системного Caddy панели, а домены транспортов проксируются на loopback-порты.
- SSH-деплой по умолчанию не проверяет host key (`InsecureIgnoreHostKey`). Для production передайте свой `ssh.HostKeyCallback` через `provisioner.Options` или используйте доверенную сеть.


Сборка вспомогательных бинарников:

```bash
CGO_ENABLED=0 go build -o bin/ ./cmd/...
```

### Удаление

```bash
sudo /opt/shampoore/uninstall.sh --purge
```

Флаг `--purge` удаляет бинарники, systemd-сервис, конфигурации, состояние и системного пользователя. Перед удалением `panel.db` скрипт пытается очистить все ноды из базы через SSH (`--cleanup-all-nodes`, best effort: недоступная VPS не блокирует purge). Если вы хотите сохранить конфигурации, запустите скрипт без флага: `sudo /opt/shampoore/uninstall.sh`.

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