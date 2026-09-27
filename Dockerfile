# full Go toolchain
FROM golang:1.26-alpine AS build
WORKDIR /app

# Copy go.mod first and download deps 
COPY go.mod ./
RUN go mod download


# Copy rest of the source and build a static Linux binary
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o distdb .


#Run stage
FROM alpine:latest
WORKDIR /app
COPY --from=build /app/distdb .

EXPOSE 8080
# ENTRYPOINT is the program; any `docker run ... <args>` are appended as its flags.
ENTRYPOINT ["./distdb"]