#!/bin/sh
set -e

SERVICE="${HERALD_SERVICE:-server}"

case "$SERVICE" in
  server)
    exec java ${JAVA_OPTS:-} -jar /app/server.jar
    ;;
  producer)
    exec java ${JAVA_OPTS:-} -jar /app/producer.jar
    ;;
  consumer)
    exec java ${JAVA_OPTS:-} -jar /app/consumer.jar
    ;;
  *)
    echo "unknown HERALD_SERVICE=$SERVICE (expected server|producer|consumer)" >&2
    exit 1
    ;;
esac
