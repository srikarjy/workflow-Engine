# Builds cmd/worker (the deployable service: worker pool + dashboard +
# /metrics) and cmd/migrate (run once, before the worker starts, to apply
# schema). Multi-stage so the final image has no Go toolchain in it.
FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/worker ./cmd/worker
RUN CGO_ENABLED=0 go build -o /out/migrate ./cmd/migrate

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=build /out/worker /out/migrate /usr/local/bin/
COPY migrations /migrations
COPY docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

EXPOSE 8080
ENTRYPOINT ["docker-entrypoint.sh"]
