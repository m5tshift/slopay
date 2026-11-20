# Build context: repository root (AGENTS.md, "Сборка образов").
FROM golang:1.27.4@sha256:205c7636d74e0ffbe3d007162df7d74f859f5a8c8322dfb9f907977686c126cf AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
# pkg/kafka embeds the event schemas of the contracts package.
COPY contracts/schemas.go contracts/
COPY contracts/schemas/ contracts/schemas/
COPY pkg/ pkg/
COPY services/chain-watcher/ services/chain-watcher/
RUN CGO_ENABLED=0 go build -trimpath -o /app ./services/chain-watcher/cmd/chain-watcher

FROM gcr.io/distroless/static:nonroot@sha256:aafe993665bf6200db38a02c11a9351e782a84b6a45e7f33b6695638c88e3931
COPY --from=build /app /app
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/app"]
