#!/bin/bash
echo "Starting DTQ services..."
docker-compose up -d --build

echo "Waiting for services to be ready..."
sleep 10

echo "Checking service health..."
docker-compose ps

echo "DTQ is ready!"
echo "RabbitMQ Management: http://localhost:15672 (guest/guest)"
echo "Redis: localhost:6379"
echo "Web Monitor: http://localhost:8000"
echo ""
echo "To view logs: docker-compose logs -f worker"
echo "To stop: docker-compose down"
