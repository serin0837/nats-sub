FROM golang:1.16

WORKDIR /app

COPY ./ /app

RUN go mod download

RUN go get -v github.com/cosmtrek/air

ENTRYPOINT ["air"]