FROM registry-mirror.pingcap.net/library/golang:1.23 as builder

WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . /build
RUN go build -o /schrddl


FROM registry-mirror.pingcap.net/library/debian:bookworm

RUN apt -y update && apt -y install wget curl \
 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /schrddl /usr/local/bin/schrddl

ENTRYPOINT ["/usr/local/bin/schrddl"]

# hub.pingcap.net/qa/schrddl
