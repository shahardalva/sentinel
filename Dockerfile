# syntax=docker/dockerfile:1

# Keep in step with the go directive in go.mod; CI checks that they match.
ARG GO_VERSION=1.27.0

FROM golang:${GO_VERSION}-alpine AS build
WORKDIR /src
# go.sum* so the build works before and after the first dependency.
COPY go.mod go.sum* ./
# A layer of its own, so a source change doesn't download modules again.
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY cmd ./cmd
COPY internal ./internal
ENV CGO_ENABLED=0
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags="-s -w" -o /out/gateway ./cmd/gateway && \
    go build -trimpath -ldflags="-s -w" -o /out/mockupstream ./cmd/mockupstream

# distroless/static ships CA certificates and a nonroot user (65532:65532),
# and has no shell or package manager.
FROM gcr.io/distroless/static-debian12:nonroot AS mockupstream
COPY --from=build /out/mockupstream /mockupstream
USER nonroot:nonroot
EXPOSE 9090
ENTRYPOINT ["/mockupstream"]

# Last, so a plain `docker build .` produces the gateway.
FROM gcr.io/distroless/static-debian12:nonroot AS gateway
COPY --from=build /out/gateway /gateway
USER nonroot:nonroot
EXPOSE 8081
ENTRYPOINT ["/gateway"]
