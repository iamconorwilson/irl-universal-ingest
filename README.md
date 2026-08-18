# IRL Universal Ingest

IRL Universal Ingest is a multi-protocol ingest server, wrapping several ingest libraries and piping their output to a single UDP stream for use in OBS or other broadcast software. It accepts incoming live streams across SRT, SRTLA, RIST, and RTMP protocols, manages stream sessions on a first-come-first-served basis, and relays the active feed to a single UDP destination.

---

## Features

- **Multi-Protocol Ingest**: Native support for SRT, SRTLA, RIST, and RTMP.
- **Session Arbitration**: First-come-first-served session locking. When a stream disconnects or goes silent beyond the configured timeout, the active slot is automatically released for the next incoming broadcast.
- **Zero-Transcode Passthrough**: Streams are muxed and forwarded using `ffmpeg -c copy` over UDP MPEG-TS, preserving original video (H.264/HEVC/AV1) and audio streams without CPU overhead.
- **Unified Path Authorization**: Cross-protocol stream path filtering with support for protocol-level encryption (SRT passphrase, RIST secret).
- **Telemetry & Monitoring**: Built-in HTTP server providing real-time session metrics (bitrate, uptime, RTT, active protocol).

---

## Default Network Ports

| Protocol | Default Port | Transport | Purpose / Client Ingest URL |
| :--- | :--- | :--- | :--- |
| **RTMP** | `1935` | TCP | `rtmp://<host>:1935/live/stream` |
| **SRT** | `8890` | UDP | `srt://<host>:8890?streamid=publish/live/stream` |
| **SRTLA** | `5000` | UDP | `srtla://<host>:5000?streamid=publish/live/stream` |
| **RIST** | `5001` | UDP | `rist://<host>:5001?username=/live/stream` |
| **UDP Output** | `8888` | UDP | Output stream destination (`udp://127.0.0.1:8888`) for OBS Media Source |
| **HTTP Stats** | `8080` | TCP | Health and telemetry API (`/stats`, `/healthz`) |

---

## Configuration

IRL Universal Ingest is configured using a YAML file (default: `config.yaml`). You can specify a custom config path using the `-config` flag when launching the server.

### Example `config.yaml`

```yaml
rtmp:
  port: 1935

srt:
  port: 8890
  latency_ms: 200
  latency_max_ms: 5000
  passphrase: ""        # Optional SRT encryption passphrase (min 10 characters)
  player_port: 8190     # Internal player port for irl-srt-server relay
  http_port: 8181       # Internal HTTP webhook port for irl-srt-server

srtla:
  port: 5000

rist:
  port: 5001
  buffer_ms: 1000
  profile: "main"       # RIST profile: "simple", "main", or "advanced"
  secret: ""            # Optional RIST authentication secret

output:
  host: "127.0.0.1"     # Target host for UDP MPEG-TS stream (use "host.docker.internal" for container dev)
  port: 8888

source_timeout: "10s"   # Silence/inactivity timeout before releasing the active stream slot
stats_port: 8080        # Port for HTTP metrics and health endpoints
log_level: "info"       # Logging verbosity: "debug", "info", "warn", "error"

auth:
  # Allowed ingest paths. Leave empty ([]) to permit any path.
  allowed_paths:
    - "/live/stream"
```

### Configuration Reference

| Key | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `rtmp.port` | `int` | `1935` | TCP port for the RTMP ingest server. |
| `srt.port` | `int` | `8890` | UDP port for direct SRT ingest. |
| `srt.latency_ms` | `int` | `200` | Minimum SRT buffer latency in milliseconds. |
| `srt.latency_max_ms` | `int` | `5000` | Maximum SRT buffer latency in milliseconds. |
| `srt.passphrase` | `string` | `""` | Optional passphrase for SRT stream encryption. |
| `srt.player_port` | `int` | `8190` | Local port used internally to pull MPEG-TS from `irl-srt-server`. |
| `srt.http_port` | `int` | `8181` | Local port used by `irl-srt-server` to deliver webhook state updates. |
| `srtla.port` | `int` | `5000` | UDP port for incoming bonded SRTLA connections via `srtla_rec`. |
| `rist.port` | `int` | `5001` | Primary UDP port for RIST stream reception via `ristreceiver`. |
| `rist.buffer_ms` | `int` | `1000` | RIST retransmission buffer size in milliseconds. |
| `rist.profile` | `string` | `"main"` | RIST profile mode (`simple`, `main`, `advanced`). |
| `rist.secret` | `string` | `""` | Optional authentication secret for RIST streams. |
| `output.host` | `string` | `"127.0.0.1"` | Destination host for relayed UDP MPEG-TS packets. |
| `output.port` | `int` | `8888` | Destination UDP port for the output stream. |
| `source_timeout` | `duration` | `"10s"` | Inactivity duration before an idle stream session is terminated. |
| `stats_port` | `int` | `8080` | HTTP port for `/stats` and `/healthz` endpoints. |
| `log_level` | `string` | `"info"` | Logging verbosity level. |
| `auth.allowed_paths` | `list` | `[]` | List of authorized stream paths (e.g., `["/live/stream"]`). An empty list permits all paths. |

---

## Development Environment

The project provides a containerized development environment with all required C/C++ libraries, FFmpeg, and Go preinstalled.

### Running with Docker Compose

1. Start the development container:
   ```bash
   docker compose -f docker-compose.dev.yml up
   ```
2. The server will start with hot-reloadable volume mounts mapping your local workspace to `/app`.
3. If running on Windows or macOS Docker Desktop, set `output.host` in `config.yaml` to `host.docker.internal` so UDP packets reach OBS running on the host machine.

### Running Tests

Execute the Go test suite inside the running development container:

```bash
docker compose -f docker-compose.dev.yml exec app go test -v ./...
```

### Testing Ingest Streams

You can simulate stream sources using FFmpeg:

```bash
# Test RTMP Ingest
ffmpeg -re -f lavfi -i testsrc=size=1280x720:rate=30 -f lavfi -i sine=frequency=1000:sample_rate=48000 \
  -c:v libx264 -preset ultrafast -tune zerolatency -c:a aac -f flv rtmp://localhost:1935/live/stream

# Test SRT Ingest
ffmpeg -re -f lavfi -i testsrc=size=1280x720:rate=30 -f lavfi -i sine=frequency=1000:sample_rate=48000 \
  -c:v libx264 -preset ultrafast -tune zerolatency -c:a aac -f mpegts "srt://localhost:8890?streamid=publish/live/stream"

# Test RIST Ingest
ffmpeg -re -f lavfi -i testsrc=size=1280x720:rate=30 -f lavfi -i sine=frequency=1000:sample_rate=48000 \
  -c:v libx264 -preset ultrafast -tune zerolatency -c:a aac -f rist "rist://localhost:5001?cname=live/stream"
```

---

## Production Deployment & Compilation

### Architecture & External Dependencies

The `irl-ingest` Go binary acts as a central orchestrator. It handles RTMP streams natively in Go, and invokes the other protocol handlers and the relay as managed child subprocesses:

- **`ffmpeg`** — remuxes and relays output via UDP MPEG-TS (`apt install ffmpeg`).
- **`irl-srt-server`** — handles incoming SRT streams and state webhooks.
- **`srtla_rec`** — proxies bonded SRTLA connections to `irl-srt-server`.
- **`ristreceiver`** — receives incoming RIST streams.

### Installing Dependencies

On Ubuntu 24.04 LTS, install system build tools and compile the external libraries:

```bash
# 1. System packages & FFmpeg
sudo apt-get update && sudo apt-get install -y \
  ffmpeg build-essential cmake pkg-config libssl-dev zlib1g-dev meson ninja-build libcjson-dev tcl

# 2. irlserver/srt & irl-srt-server
git clone --depth 1 -b belabox https://github.com/irlserver/srt.git /tmp/srt && \
  cd /tmp/srt && ./configure --prefix=/usr/local && make -j$(nproc) && sudo make install && sudo ldconfig
git clone --depth 1 https://github.com/irlserver/irl-srt-server.git /tmp/irl-srt-server && \
  cd /tmp/irl-srt-server && git submodule update --init && \
  cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build -j$(nproc) && \
  sudo cp build/bin/* /usr/local/bin/

# 3. srtla (srtla_rec)
git clone --depth 1 https://github.com/irlserver/srtla.git /tmp/srtla && \
  cd /tmp/srtla && git submodule update --init && \
  cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build -j$(nproc) && \
  sudo cp build/srtla_rec /usr/local/bin/

# 4. VideoLAN librist (ristreceiver)
git clone --depth 1 https://code.videolan.org/rist/librist.git /tmp/librist && \
  cd /tmp/librist && meson setup build --prefix=/usr/local -Dbuilt_tools=true && \
  ninja -C build && sudo ninja -C build install && sudo ldconfig
```

### Compiling the Binary

Compile a stripped, production-ready Go binary:

```bash
go build -ldflags="-s -w" -o irl-ingest ./cmd/server
```

### Running the Binary

Launch the server with your configuration:

```bash
./irl-ingest -config ./config.yaml
```

---

## Monitoring & Telemetry

### `GET /stats`

Returns real-time telemetry for all configured and active ingest paths:

```json
[
  {
    "path": "/live/stream",
    "active": true,
    "protocol": "SRT",
    "uptime": 142,
    "bitrate": 6240,
    "RTT": 45
  }
]
```

### `GET /healthz`

Returns HTTP 200 `ok` when the server is healthy and accepting streams.

---

## Acknowledgements

- **[irl-srt-server](https://github.com/irlserver/irl-srt-server)** - Core SRT & SRTLA ingest handling.
- **[srtla](https://github.com/irlserver/srtla)** - SRTLA bonding receiver proxy implementation (`srtla_rec`).
- **[librist](https://code.videolan.org/rist/librist)** - RIST streaming protocol implementation (`ristreceiver`).
- **[gortmplib](https://github.com/bluenviron/gortmplib)** - Go-native RTMP ingest handling.
- **[FFmpeg](https://ffmpeg.org)** - Stream remuxing and low-latency UDP relay.


## Issues

There will be many, please report them in the issues section.
Feel free to try this out in your own setup if you think it could be useful. I wouldn't use this for production environments, as it remains not fully tested.