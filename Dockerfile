FROM golang:1.27.3-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/federation-api ./cmd/federation-api

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /
COPY --from=build /out/federation-api /federation-api
EXPOSE 8081
USER nonroot:nonroot
ENTRYPOINT ["/federation-api"]
