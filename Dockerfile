FROM golang:1.25-bookworm AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/irl-ingest ./cmd/server

FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    ca-certificates \
    build-essential \
    cmake \
    pkg-config \
    libssl-dev \
    zlib1g-dev \
    meson \
    ninja-build \
    libcjson-dev \
    tcl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Build and install irlserver/srt (belabox branch)
RUN git clone --depth 1 -b belabox https://github.com/irlserver/srt.git /tmp/srt && \
    cd /tmp/srt && \
    ./configure --prefix=/usr/local && \
    make -j$(nproc) && \
    make install && \
    ldconfig && \
    rm -rf /tmp/srt

# Build and install irlserver/irl-srt-server
RUN git clone --depth 1 https://github.com/irlserver/irl-srt-server.git /tmp/irl-srt-server && \
    cd /tmp/irl-srt-server && \
    git submodule update --init && \
    cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && \
    cmake --build build -j$(nproc) && \
    cp build/bin/* /usr/local/bin/ && \
    rm -rf /tmp/irl-srt-server

# Build and install irlserver/srtla (srtla_rec)
RUN git clone --depth 1 https://github.com/irlserver/srtla.git /tmp/srtla && \
    cd /tmp/srtla && \
    git submodule update --init && \
    cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && \
    cmake --build build -j$(nproc) && \
    cp build/srtla_rec /usr/local/bin/ && \
    rm -rf /tmp/srtla

# Build and install VideoLAN librist (ristreceiver)
RUN git clone --depth 1 https://code.videolan.org/rist/librist.git /tmp/librist && \
    cd /tmp/librist && \
    meson setup build --prefix=/usr/local -Dbuilt_tools=true && \
    ninja -C build install && \
    ldconfig && \
    rm -rf /tmp/librist

# Toolchain and sources are no longer needed at runtime.
RUN apt-get purge -y build-essential cmake pkg-config libssl-dev zlib1g-dev meson ninja-build tcl git && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /out/irl-ingest /usr/local/bin/irl-ingest

EXPOSE 1935 8080 8890/udp 5000/udp 5001/udp 8888/udp

ENTRYPOINT ["/usr/local/bin/irl-ingest"]
CMD ["-config", "/app/config.yaml"]
