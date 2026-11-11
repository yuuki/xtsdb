# syntax=docker/dockerfile:1

FROM --platform=$BUILDPLATFORM golang:1.27.4 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
ARG REVISION=
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath \
      -ldflags "-s -w -X github.com/yuuki/xtsdb/internal/version.Version=${VERSION} -X github.com/yuuki/xtsdb/internal/version.Revision=${REVISION}" \
      -o /out/xtsdb ./cmd/xtsdb

FROM gcr.io/distroless/static-debian13:nonroot
COPY --from=build /out/xtsdb /usr/local/bin/xtsdb
COPY examples/xtsdb.yaml /etc/xtsdb/xtsdb.yaml
# 9201: HTTP (remote write, query API, metrics), 2003: Graphite plaintext
EXPOSE 9201 2003 2003/udp
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/xtsdb"]
CMD ["all", "-config.file=/etc/xtsdb/xtsdb.yaml", "-config.expand-env"]
