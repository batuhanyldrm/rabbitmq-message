FROM golang:1.24

WORKDIR /app

# wait-for-it scriptini ekle
COPY wait-for-it.sh /wait-for-it.sh
RUN chmod +x /wait-for-it.sh

# Go modülleri
COPY go.mod go.sum ./
RUN go mod download

# Uygulama dosyaları
COPY . .

# Build
RUN go build -o api .

# RabbitMQ portunu bekledikten sonra başlat
CMD ["/wait-for-it.sh", "rabbitmq:5672", "--timeout=30", "--", "./api"]
