#!/usr/bin/env bash
# ============================================================
# Kafka CLI - Gestión de tópicos
# Uso: ./kafka-cli-topics.sh [list|create|describe|delete|reset]
# ============================================================
set -euo pipefail

CONTAINER="gym-kafka"
BOOTSTRAP_INTERNAL="gym-kafka:29092"

# Wrapper: ejecuta un comando kafka-* dentro del contenedor
k() {
  docker exec -it "$CONTAINER" "$@"
}

case "${1:-}" in
  list)
    echo ">>> Tópicos existentes:"
    k kafka-topics --bootstrap-server "$BOOTSTRAP_INTERNAL" --list
    ;;

  create)
    echo ">>> Creando tópicos del gimnasio..."

    k kafka-topics --bootstrap-server "$BOOTSTRAP_INTERNAL" \
      --create --if-not-exists \
      --topic gym.reservas.eventos.v1 \
      --partitions 3 \
      --replication-factor 1 \
      --config retention.ms=604800000 \
      --config cleanup.policy=delete

    k kafka-topics --bootstrap-server "$BOOTSTRAP_INTERNAL" \
      --create --if-not-exists \
      --topic gym.clases.eventos.v1 \
      --partitions 3 \
      --replication-factor 1 \
      --config retention.ms=604800000 \
      --config cleanup.policy=delete

    k kafka-topics --bootstrap-server "$BOOTSTRAP_INTERNAL" \
      --create --if-not-exists \
      --topic gym.notificaciones.v1 \
      --partitions 1 \
      --replication-factor 1 \
      --config retention.ms=259200000 \
      --config cleanup.policy=delete

    echo ">>> Tópicos creados."
    k kafka-topics --bootstrap-server "$BOOTSTRAP_INTERNAL" --list
    ;;

  describe)
    TOPIC="${2:?Uso: $0 describe <topic>}"
    k kafka-topics --bootstrap-server "$BOOTSTRAP_INTERNAL" --describe --topic "$TOPIC"
    ;;

  delete)
    TOPIC="${2:?Uso: $0 delete <topic>}"
    k kafka-topics --bootstrap-server "$BOOTSTRAP_INTERNAL" --delete --topic "$TOPIC"
    echo ">>> Tópico $TOPIC eliminado."
    ;;

  reset)
    echo ">>> Reiniciando todos los tópicos del gimnasio (borrar y recrear)..."
    for t in gym.reservas.eventos.v1 gym.clases.eventos.v1 gym.notificaciones.v1; do
      k kafka-topics --bootstrap-server "$BOOTSTRAP_INTERNAL" --delete --topic "$t" 2>/dev/null || true
    done
    sleep 2
    "$0" create
    ;;

  *)
    echo "Uso: $0 [list|create|describe <topic>|delete <topic>|reset]"
    exit 1
    ;;
esac