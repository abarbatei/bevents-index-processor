@echo off
set RABBIT_HOST_URL=localhost
set RABBIT_HOST_PORT=5672
set RABBIT_EXCHANGE=events
set RABBIT_ROUTING_KEY=events_test.all
set RABBIT_QUEUE_NAME=events_test
set RABBIT_USER=guest
set RABBIT_PASSWORD=guest
set MONGO_DB_CONNECTION_STRING=mongodb://localhost:27017
py.test