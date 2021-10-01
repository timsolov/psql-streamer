FROM golang:1.17.1-alpine3.13 AS build-env
ADD . /src
WORKDIR /src
RUN apk update && apk add musl-dev make gcc && make build

FROM alpine:3.13.5

WORKDIR /app
COPY --from=build-env /src/.out/psql-streamer /app/
