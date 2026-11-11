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
  <a href="https://github.com/Esteviredzu/shampoore/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-GPL-green?style=flat-square" alt="License"></a>
</p>

---

## Обзор

**shampoore** - это инструмент для туннелирования трафика, который маскирует его под легитимные запросы к популярным сервисам. В отличие от классических VPN, shampoore использует публичные API (Yandex, LiveKit и др.) в качестве транспорта.

Проект включает в себя:
- **shampoore-server** - демон, который принимает туннелированный трафик и перенаправляет его в интернет.
- **shampoore-panel** - встроенная веб-панель для управления сервером и конфигурациями.
- **shampoore-client** - CLI-клиент для локального проксирования (SOCKS5 или TUN).
- **Mobile SDK** - готовый AAR для интеграции в Android-приложения.

## Возможности

- **Множество транспортов**:
  - `yandex` (Yandex Board)
  - `yandex_telemost_raw` (Telemost + raw VP8)
  - `yandex_telemost_squares` (Telemost + framing)
  - `livekit_datachannel` (LiveKit Reliable DataChannel)
  - `multi` (MultiPipe: агрегация нескольких транспортов через `flowmux` или `bond`)
- **Безопасность**: Шифрование XChaCha20-Poly1305, FEC (Reed-Solomon) и надежный транспорт на базе KCP.
- **Веб-панель**: Современный UI в стиле Material 3, управление конфигами, QR-коды для экспорта, мониторинг CPU/RAM.
- **Режимы клиента**: Локальный SOCKS5-прокси или полноценный VPN (TUN).

## Архитектура

Веб-панель не содержит логики туннелирования. Она общается с `shampoore-server` через Unix-сокет.
```text
[ Клиент ] -> [ shampoore-server ] -> [ Публичный сервис (Yandex/LiveKit) ]
                      ^
                      | Unix Socket
                [ shampoore-panel ]
```

## Установка

Поддерживаются системы на базе Linux (systemd). Для установки сервера и панели одной командой выполните:

```bash
curl -fsSL https://github.com/Esteviredzu/shampoore/releases/latest/download/install.sh | sudo bash
```

Скрипт автоматически:
1. Создаст системного пользователя `shampoore`.
2. Установит бинарники в `/opt/shampoore/bin`.
3. Сгенерирует токен доступа и настроит `/etc/shampoore/panel.env`.
4. Запустит systemd-сервис `shampoore.service` с автозапуском при перезагрузке.

### Доступ к панели

По умолчанию панель слушает `127.0.0.1:8080` и недоступна извне. Для безопасного доступа используйте SSH-туннель

```bash
ssh -L 8080:127.0.0.1:8080 user@your-server
```

Оставьте это окно терминала открытым, затем откройте в браузере: `http://localhost:8080/?token=<ТОКЕН_ИЗ_ВЫВОДА_УСТАНОВЩИКА>`.

### Удаление

```bash
sudo /opt/shampoore/uninstall.sh --purge
```

Флаг `--purge` удаляет бинарники, systemd-сервис, конфигурации, состояние и системного пользователя. Если вы хотите сохранить конфигурации, запустите скрипт без флага: `sudo /opt/shampoore/uninstall.sh`.

## Использование клиента

Клиент может принимать JSON-файл или URL формата `shampoore://`.

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

Для мобильных приложений доступна скомпилированная библиотека AAR. Скачать её можно из раздела Releases.

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

Я вдохновился [olcrtc](https://github.com/openlibrecommunity/olcrtc) и [OpenFlux](https://github.com/p1neapplexpress/OpenFlux) и захотел наколхозить что-то своё.