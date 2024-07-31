FROM golang:1.21 AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY api ./api
COPY cmd ./cmd
COPY internal ./internal
COPY pkg ./pkg
RUN CGO_ENABLED=0 GOOS=linux go build -o /docker-broker ./cmd/broker/main.go



FROM build AS run-test-stage
RUN go test -v ./...



FROM alpine:3.20 AS release
COPY --from=build /docker-broker /docker-broker
RUN chmod +x /docker-broker

EXPOSE 8080
EXPOSE 50051

ENTRYPOINT ["/docker-broker"]