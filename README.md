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

- **Linux** - CLI, SOCKS5, полноценный TUN, веб-панель управления.
- **Android** - сборка в AAR через `gomobile`. Поддерживается SOCKS5 и TUN через системный `VpnService` fd.

## Существующие транспорты

- **Yandex Board** - передача данных через интерактивные доски.
- **Yandex Telemost raw** - передача через VP8-видеопоток в конференции Телемоста.
- **Yandex Telemost squares** - передача через VP8, данные кодируются сеткой квадратов в видеокадре.

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

Требования: Go >= 1.23, Android NDK, gomobile.

```bash
go install golang.org/x/mobile/cmd/gomobile@latest
go install golang.org/x/mobile/cmd/gobind@latest
gomobile init

export ANDROID_NDK_HOME=/path/to/ndk
./build-aar.sh
```

Результат сборки: `shampoore.aar` и `shampoore-sources.jar`.

## Формат конфигов

Конфиг представляет собой json.

```json
{
  "name": "название",
  "type": "yandex",
  "transport": { "hash": "..." },
  "encryption_password": "VERY HARD PASSWORD",
  "advanced": { "tun_mtu": 1400 }
}
```

- **`name`** - имя.
- **`type`** - тип (`yandex`, `yandex_telemost_raw`, `yandex_telemost_squares`).
- **`transport`** - специфичные для транспорта параметры.
- **`encryption_password`** - пароль для шифрования XChaCha20-Poly1305. Должен совпадать на клиенте и сервере.
- **`advanced`** - словарь для переопределения дефолтных настроек.

### Где что лежит

- В папке **`examples/`** лежат готовые примеры конфигов - берите их за основу.
- Свои конфиги складывайте в папку **`configs/`**.

## Запуск

### Режим с веб-панелью

Запустите бинарник `shampoore-panel` и откройте `http://127.0.0.1:8080`. Панель сама поднимет `shampoore-server` и будет управлять конфигами.

### CLI

Вариант 1 - перечислить файлы:

```bash
./bin/shampoore-server -c config1.json -c config2.json
```

Вариант 2 - указать директорию с конфигами:

```bash
./bin/shampoore-server --configs-dir ./configs
```

Тогда сервер возьмёт все `*.json` из этой папки и запустит все сразу.

### Запуск клиента
## Linux
```bash
# SOCKS5
./bin/shampoore-client -c config.json -m socks5

# TUN
sudo ./bin/shampoore-client -c config.json -m tun
```

Флаги клиента:
- `-c` - путь к файлу конфига или `shampoore://` URL.
- `-m` - режим работы: `socks5` (по умолчанию) или `tun`.

Проверка:
```bash
curl -4 --socks5-hostname 127.0.0.1:1080 https://ifconfig.me
curl -4 https://ifconfig.me
```

## Документация

Настройки `advanced` и мобильный API - лежит в [TECHNICAL.md](TECHNICAL.md).

## О проекте

Я вдохновился [olcrtc](https://github.com/openlibrecommunity/olcrtc) и [OpenFlux](https://github.com/p1neapplexpress/OpenFlux) и захотел наколхозить что-то своё.