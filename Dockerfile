# Multi-stage: the toolchain never ships. The build stage needs a Go compiler,
# module cache, and source tree (~400MB); the runtime needs one static binary.
FROM golang:1.26-alpine AS build
WORKDIR /src

# Copy manifests first so the dependency layer is cached independently of
# source changes -- editing a handler shouldn't re-download every module.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# CGO_ENABLED=0 produces a static binary with no libc dependency, which is what
# lets the final image be distroless/static with nothing else in it.
# -s -w strip the symbol table and DWARF info; nothing debugs by attaching to a
# production container here.
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/cogs ./cmd/cogs

# distroless/static: no shell, no package manager, no busybox. An attacker who
# achieves RCE lands in an image with nothing to pivot with, and the image is
# ~15MB rather than ~200MB.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/cogs /cogs
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/cogs"]
