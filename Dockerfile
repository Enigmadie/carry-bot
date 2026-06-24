# Multi-stage build: the Go toolchain lives only in the build stage, so neither
# the runtime image nor the ThinkCentre host ever needs Go installed. One image
# ships all four service binaries; compose selects which to run via `command`.

FROM golang:1.26-alpine AS build
WORKDIR /src

# Download modules in their own layer so source edits don't re-fetch deps.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# CGO off → fully static binaries (no libc at runtime). Our deps are pure Go
# (nats.go, coder/websocket), so nothing needs cgo.
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/market-data ./services/market-data \
 && CGO_ENABLED=0 GOOS=linux go build -o /out/strategy    ./services/strategy \
 && CGO_ENABLED=0 GOOS=linux go build -o /out/order        ./services/order \
 && CGO_ENABLED=0 GOOS=linux go build -o /out/portfolio    ./services/portfolio

FROM alpine:3.21
# ca-certificates: outbound TLS to Bybit (wss:// market data, https:// REST)
# needs the system CA bundle, which the scratch/distroless-free alpine lacks.
RUN apk add --no-cache ca-certificates
COPY --from=build /out/ /usr/local/bin/

# No ENTRYPOINT on purpose: each compose service sets `command` to one of
# market-data | strategy | order | portfolio, all resolvable on PATH from
# /usr/local/bin.
