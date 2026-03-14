FROM golang:1.24-alpine AS builder

WORKDIR /src

ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
ARG VERSION_HASH=unknown

RUN apk add --no-cache build-base ca-certificates tzdata

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=1 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
  go build -trimpath \
  -ldflags="-s -w -X github.com/komari-monitor/komari/utils.CurrentVersion=${VERSION} -X github.com/komari-monitor/komari/utils.VersionHash=${VERSION_HASH}" \
  -o /out/komari .

FROM alpine:3.21

WORKDIR /app

COPY --from=builder /etc/ssl/certs /etc/ssl/certs
COPY --from=builder /etc/ssl/cert.pem /etc/ssl/cert.pem
COPY --from=builder /usr/share/ca-certificates /usr/share/ca-certificates
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=builder /out/komari /app/komari

ENV GIN_MODE=release
ENV KOMARI_DB_TYPE=sqlite
ENV KOMARI_DB_FILE=/app/data/komari.db
ENV KOMARI_DB_HOST=localhost
ENV KOMARI_DB_PORT=3306
ENV KOMARI_DB_USER=root
ENV KOMARI_DB_PASS=
ENV KOMARI_DB_NAME=komari
ENV KOMARI_LISTEN=0.0.0.0:25774

EXPOSE 25774

CMD ["/app/komari", "server"]
