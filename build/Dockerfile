FROM golang:1.17.6-alpine3.15 AS build-env
ADD . /src
WORKDIR /src
RUN apk update && apk add musl-dev make gcc && make build

FROM alpine:3.15
WORKDIR /app
COPY --from=build-env /src/.out/psql-streamer /app/
