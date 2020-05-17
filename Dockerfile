FROM golang:alpine3.11
MAINTAINER michalski luc <michalski.luc@gmail.com>

RUN apk add --no-cache nano bash jq gcc musl-dev sqlite-dev sqlite git

WORKDIR /app
COPY go.mod .
COPY main.go .

# RUN go build -o ./dmoz-converter main.go

CMD ["/bin/bash"]
