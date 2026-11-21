# Fetcher: USGS -> NATS. Static binary on distroless (has CA certs for the HTTPS feed).
FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /fetcher .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /fetcher /fetcher
ENTRYPOINT ["/fetcher"]
