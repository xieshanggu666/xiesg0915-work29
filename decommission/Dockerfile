FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/server ./cmd/server && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/cli ./cmd/cli

FROM alpine:3.20
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 eraser
USER eraser
COPY --from=build /out/server /usr/local/bin/decommission-server
COPY --from=build /out/cli /usr/local/bin/decommission-cli
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/decommission-server"]
