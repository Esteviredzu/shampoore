# shampoore-core: advanced и мобильный API

## Advanced

Ключи из `advanced` перебивают дефолты. Если не понимаешь, что делает параметр - лучше не трогай, дефолты подобраны нормально.

### Общее

```json
{
  "advanced": {
    "socks5_listen": "127.0.0.1:1080",
    "tun_name": "shampoore0",
    "tun_mtu": 1500,
    "tun_inet4": "172.19.0.1/30",
    "tun_auto_route": true,
    "tun_dns": "1.1.1.1:53,8.8.8.8:53",
    "tun_bypass": "example.com,another.host"
  }
}
```

- `socks5_listen` - где слушать SOCKS5 в режиме `-m socks5`. В `tun` не используется.
- `tun_name` - имя TUN-интерфейса.
- `tun_mtu` - MTU. Работает на Linux CLI, на Android задаётся через `VpnService.Builder`.
- `tun_inet4` - IP и маска для TUN.
- `tun_auto_route` - перебивать маршрут по умолчанию или нет. Если `false`, TUN поднимется, но маршруты не тронет.
- `tun_dns` - DNS-серверы для резолва bypass-хостов, через запятую.
- `tun_bypass` - список хостов/IP, которые идут мимо туннеля.

### Yandex Board

```json
{
  "advanced": {
    "mux_flush_ms": 20,
    "mux_flush_threshold": 12000,
    "reliable_window": 64,
    "reliable_chunk": 0,
    "padding_enabled": true,
    "padding_target": 14000,
    "jitter_min_ms": 0,
    "jitter_max_ms": 0
  }
}
```

- `mux_flush_ms` - как часто мультиплексор сбрасывает накопленные кадры.
- `mux_flush_threshold` - порог в байтах, после которого отправляет сразу, не дожидаясь таймера.
- `reliable_window` - сколько пакетов может быть в полёте одновременно.
- `reliable_chunk` - размер чанка, 0 - авто.
- `padding_enabled` - включить паддинг.
- `padding_target` - до скольки байт добивать паддингом.
- `jitter_min_ms` / `jitter_max_ms` - искусственный джиттер.

### Yandex Telemost raw

```json
{
  "advanced": {
    "mux_flush_ms": 10,
    "mux_flush_threshold": 12000,
    "kcp_conv": 0,
    "kcp_mtu": 1200,
    "kcp_window": 1024,
    "kcp_nodelay": 1,
    "kcp_interval": 10,
    "kcp_resend": 2,
    "kcp_nc": true,
    "fec_enabled": false,
    "fec_data_shards": 10,
    "fec_parity_shards": 3,
    "raw_frame_size": 12000,
    "raw_fps": 60,
    "raw_copies": 1,
    "raw_gop": 30
  }
}
```

### Yandex Telemost squares

```json
{
  "advanced": {
    "mux_flush_ms": 10,
    "mux_flush_threshold": 4000,
    "kcp_conv": 0,
    "kcp_mtu": 390,
    "kcp_window": 512,
    "kcp_nodelay": 1,
    "kcp_interval": 5,
    "kcp_resend": 2,
    "kcp_nc": true,
    "fec_enabled": false,
    "fec_data_shards": 10,
    "fec_parity_shards": 3,
    "squares_bits_per_cell": 4,
    "squares_cell_size": 16,
    "squares_cols": 120,
    "squares_rows": 80,
    "squares_fps": 45,
    "squares_bitrate": "4000K",
    "squares_copies": 1
  }
}
```

Пояснения по общим для Телемоста ключам:

- `kcp_conv` - ID KCP-соединения, 0 - сгенерится из ID конференции.
- `kcp_mtu` - MTU для KCP. У squares заметно ниже, потому что в один кадр лезет мало.
- `kcp_window`, `kcp_nodelay`, `kcp_interval`, `kcp_resend`, `kcp_nc` - стандартные параметры KCP.
- `fec_enabled` - включить FEC (Reed-Solomon).
- `fec_data_shards` / `fec_parity_shards` - сколько data- и parity-шардов.
- `raw_*` - параметры VP8-потока для raw-транспорта.
- `squares_*` - параметры сетки: сколько бит на клетку, размер клетки, кол-во колонок и строк, FPS, битрейт, дубли.

## Мобильный API

Пакет `mobile`, дёргается через gomobile из Kotlin/Java:

```kotlin
// SOCKS5. Слушает на advanced.socks5_listen
Mobile.startClient(source: String): String

// TUN. fd и mtu приходят из VpnService
Mobile.startClientVPN(source: String, fd: Int, mtu: Int): String

// Остановка (таймаут 7 секунд)
Mobile.stopClient()

Mobile.isRunning(): Boolean
```

`source` - путь к JSON-файлу или `shampoore://`-URL.