#!/usr/bin/env bash
# ============================================================
# Kafka CLI - Productores
# Uso: ./kafka-cli-produce.sh [interactive|file|json]
# ============================================================
set -euo pipefail

CONTAINER="gym-kafka"
BOOTSTRAP_INTERNAL="gym-kafka:29092"
TOPIC_RESERVAS="gym.reservas.eventos.v1"
TOPIC_CLASES="gym.clases.eventos.v1"
DATA_DIR="$(cd "$(dirname "$0")" && pwd)/data"

k() { docker exec -it "$CONTAINER" "$@"; }

case "${1:-}" in
  # ------------------------------------------------------------
  # 1. Productor INTERACTIVO (línea por mensaje, Ctrl+D para salir)
  # ------------------------------------------------------------
  interactive)
    echo ">>> Productor interactivo en $TOPIC_RESERVAS"
    echo ">>> Escribe un mensaje por línea. Ctrl+D para salir."
    echo ">>> Cada línea debe tener formato: KEY:VALUE"
    echo ">>> Ejemplo: r-001:RESERVA_CONFIRMADA socioId=... claseId=..."
    echo ""
    k kafka-console-producer \
      --bootstrap-server "$BOOTSTRAP_INTERNAL" \
      --topic "$TOPIC_RESERVAS" \
      --property "parse.key=true" \
      --property "key.separator=:"
    ;;

  # ------------------------------------------------------------
  # 2. Productor desde ARCHIVO TEXTO (línea por mensaje, sin key)
  # ------------------------------------------------------------
  file)
    echo ">>> Produciendo desde reservas-mensajes.txt en $TOPIC_RESERVAS"
    docker exec -i "$CONTAINER" kafka-console-producer \
      --bootstrap-server "$BOOTSTRAP_INTERNAL" \
      --topic "$TOPIC_RESERVAS" \
      < "$DATA_DIR/reservas-mensajes.txt"
    echo ">>> Producción desde archivo texto completada."
    ;;

  # ------------------------------------------------------------
  # 3. Productor desde ARCHIVO JSON (con key explícita)
  # ------------------------------------------------------------
  json)
    echo ">>> Produciendo reservas-eventos.json en $TOPIC_RESERVAS (con key=reservaId)"
    # El JSON lleva el campo reservaId. Para usar key, construimos líneas "key:json"
    # con jq. Si no tienes jq en tu host, usa el fallback con sed (más simple).
    if command -v jq >/dev/null 2>&1; then
      jq -r '"\(.reservaId):\(.)"' "$DATA_DIR/reservas-eventos.json" \
        | docker exec -i "$CONTAINER" kafka-console-producer \
            --bootstrap-server "$BOOTSTRAP_INTERNAL" \
            --topic "$TOPIC_RESERVAS" \
            --property "parse.key=true" \
            --property "key.separator=:"
    else
      echo ">>> jq no está instalado. Usando productor SIN key (fallback)."
      docker exec -i "$CONTAINER" kafka-console-producer \
        --bootstrap-server "$BOOTSTRAP_INTERNAL" \
        --topic "$TOPIC_RESERVAS" \
        < "$DATA_DIR/reservas-eventos.json"
    fi
    echo ">>> Producción JSON completada."

    echo ">>> Produciendo clases-eventos.json en $TOPIC_CLASES (con key=claseId)"
    if command -v jq >/dev/null 2>&1; then
      jq -r '"\(.claseId):\(.)"' "$DATA_DIR/clases-eventos.json" \
        | docker exec -i "$CONTAINER" kafka-console-producer \
            --bootstrap-server "$BOOTSTRAP_INTERNAL" \
            --topic "$TOPIC_CLASES" \
            --property "parse.key=true" \
            --property "key.separator=:"
    else
      docker exec -i "$CONTAINER" kafka-console-producer \
        --bootstrap-server "$BOOTSTRAP_INTERNAL" \
        --topic "$TOPIC_CLASES" \
        < "$DATA_DIR/clases-eventos.json"
    fi
    echo ">>> Producción de clases completada."
    ;;

  *)
    echo "Uso: $0 [interactive|file|json]"
    exit 1
    ;;
esac