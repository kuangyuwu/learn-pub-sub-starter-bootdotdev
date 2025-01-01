#!/bin/bash

case "$1" in
    start)
        echo "Starting RabbitMQ container..."
        docker run -d --rm --name rabbitmq-stomp -p 5672:5672 -p 15672:15672 rabbitmq-stomp
        ;;
    stop)
        echo "Stopping RabbitMQ container..."
        docker stop rabbitmq-stomp
        ;;
    logs)
        echo "Fetching logs for RabbitMQ container..."
        docker logs -f rabbitmq-stomp
        ;;
    *)
        echo "Usage: $0 {start|stop|logs}"
        exit 1
esac
