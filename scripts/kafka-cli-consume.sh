#!/usr/bin/env bash
# ============================================================
# Kafka CLI - Consumers (2 grupos en paralelo sobre el mismo topic)
# Uso: ./kafka-cli-consume.sh [group-a|group-b|from-beginning|list-groups]
# ============================================================
set -euo pipefail

CONTAINER="gym-kafka"
BOOTSTRAP_INTERNAL="gym-kafka:29092"
TOPIC_RESERVAS="gym.reservas.eventos.v1"

k() { docker exec -it "$CONTAINER" "$@"; }

case "${1:-}" in
  # Consumer 1: grupo "gym-notificaciones"
  group-a)
    echo ">>> Consumer grupo gym-notificaciones (Ctrl+C para salir)"
    k kafka-console-consumer \
      --bootstrap-server "$BOOTSTRAP_INTERNAL" \
      --topic "$TOPIC_RESERVAS" \
      --group gym-notificaciones \
      --property print.key=true \
      --property print.partition=true \
      --property print.timestamp=true
    ;;

  # Consumer 2: grupo "gym-auditoria"
  group-b)
    echo ">>> Consumer grupo gym-auditoria (Ctrl+C para salir)"
    k kafka-console-consumer \
      --bootstrap-server "$BOOTSTRAP_INTERNAL" \
      --topic "$TOPIC_RESERVAS" \
      --group gym-auditoria \
      --property print.key=true \
      --property print.partition=true \
      --property print.timestamp=true
    ;;

  # Leer desde el principio (sin grupo persistente, para inspección rápida)
  from-beginning)
    echo ">>> Leyendo $TOPIC_RESERVAS desde el principio (Ctrl+C para salir)"
    k kafka-console-consumer \
      --bootstrap-server "$BOOTSTRAP_INTERNAL" \
      --topic "$TOPIC_RESERVAS" \
      --from-beginning \
      --property print.key=true \
      --property print.partition=true \
      --timeout-ms 5000
    ;;

  # Listar grupos y su estado
  list-groups)
    echo ">>> Consumer groups:"
    k kafka-consumer-groups --bootstrap-server "$BOOTSTRAP_INTERNAL" --list
    ;;

  *)
    echo "Uso: $0 [group-a|group-b|from-beginning|list-groups]"
    exit 1
    ;;
esac