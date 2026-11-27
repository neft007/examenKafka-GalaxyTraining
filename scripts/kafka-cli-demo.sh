#!/usr/bin/env bash
# ============================================================
# DEMO END-TO-END para la presentación del examen Kafka
# Automatiza: crear socio → crear clase → reservar → verificar
# Uso: ./scripts/demo-presentacion.sh [feliz|fallo|contador|todo]
# ============================================================
set -euo pipefail

API="http://localhost:8080/api/v1"
MAILHOG="http://localhost:8025/api/v2/messages"
KAFKA_UI="http://localhost:8090"
SCHEMA_REGISTRY="http://localhost:8081"
KAFKA_CONTAINER="gym-kafka"

# Colores
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log()      { echo -e "${BLUE}▶ $*${NC}"; }
log_ok()   { echo -e "${GREEN}✓ $*${NC}"; }
log_warn() { echo -e "${YELLOW}⚠ $*${NC}"; }
log_err()  { echo -e "${RED}✗ $*${NC}"; }

# ------------------------------------------------------------
# Utilidades
# ------------------------------------------------------------
require_jq() {
  if ! command -v jq >/dev/null 2>&1; then
    log_err "jq no está instalado. Instálalo con: sudo apt install jq | brew install jq"
    exit 1
  fi
}

check_stack() {
  log "Verificando que el stack esté arriba..."
  if ! curl -sf "$API/../actuator/health" >/dev/null 2>&1; then
    log_err "La API no responde en $API/../actuator/health"
    echo "  → docker compose ps"
    exit 1
  fi
  log_ok "API UP"
}

# ------------------------------------------------------------
# Crear socio (devuelve UUID por stdout)
# ------------------------------------------------------------
crear_socio() {
  local nombre="$1"
  local email="$2"
  local dni="$3"

  local body
  body=$(curl -sf -X POST "$API/socios" \
    -H "Content-Type: application/json" \
    -d "{\"dni\":\"$dni\",\"nombre\":\"$nombre\",\"email\":\"$email\"}")

  echo "$body" | jq -r '.id'
}

# ------------------------------------------------------------
# Crear clase (devuelve UUID por stdout)
# ------------------------------------------------------------
crear_clase() {
  local nombre="$1"
  local instructor="$2"
  local fecha_iso="$3"
  local duracion="${4:-60}"
  local aforo="${5:-10}"

  local body
  body=$(curl -sf -X POST "$API/clases" \
    -H "Content-Type: application/json" \
    -d "{\"nombre\":\"$nombre\",\"instructor\":\"$instructor\",\"fechaInicio\":\"$fecha_iso\",\"duracionMinutos\":$duracion,\"aforoMaximo\":$aforo}")

  echo "$body" | jq -r '.id'
}

# ------------------------------------------------------------
# Reservar plaza
# ------------------------------------------------------------
reservar() {
  local socio_id="$1"
  local clase_id="$2"

  curl -sf -X POST "$API/clases/$clase_id/reservas" \
    -H "Content-Type: application/json" \
    -d "{\"socioId\":\"$socio_id\"}" >/dev/null
}

# ------------------------------------------------------------
# Contar correos en MailHog
# ------------------------------------------------------------
mailhog_count() {
  curl -sf "$MAILHOG" | jq '.total' 2>/dev/null || echo "0"
}

# ------------------------------------------------------------
# Escenario 1: FLUJO FELIZ
# ------------------------------------------------------------
demo_feliz() {
  echo ""
  echo "============================================================"
  echo "  ESCENARIO 1 — FLUJO FELIZ (REST → Kafka → MailHog)"
  echo "============================================================"

  local antes
  antes=$(mailhog_count)
  log "Correos en MailHog antes: $antes"

  # Datos
  local ts; ts=$(date +%s)
  local dni; dni=$(printf "%08d" $((ts % 100000000)))

  log "Creando socio normal (email: feliz-$ts@example.com)..."
  local socio_id
  socio_id=$(crear_socio "Feliz $ts" "feliz-$ts@example.com" "$dni")
  log_ok "Socio: $socio_id"

  log "Creando clase futura..."
  local fecha; fecha=$(date -u -d "+2 days" +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null \
    || date -u -v+2d +"%Y-%m-%dT%H:%M:%SZ")
  local clase_id
  clase_id=$(crear_clase "Yoga Demo $ts" "Prof. Demo" "$fecha")
  log_ok "Clase: $clase_id"

  log "Reservando plaza..."
  reservar "$socio_id" "$clase_id"
  log_ok "Reserva creada"

  log "Esperando 3s para que MailHog reciba el correo..."
  sleep 3

  local despues
  despues=$(mailhog_count)
  log "Correos en MailHog después: $despues"

  if [ "$despues" -gt "$antes" ]; then
    log_ok "Correo recibido en MailHog → http://localhost:8025"
  else
    log_warn "No apareció correo nuevo. Revisa logs: docker compose logs --tail=50 gym-api"
  fi

  echo ""
  log "Verifica también:"
  echo "  → Kafka UI: $KAFKA_UI/topics/gym.reservas.eventos.v1"
  echo "  → Schema Registry: $SCHEMA_REGISTRY/subjects"
}

# ------------------------------------------------------------
# Escenario 2: FALLO CONTROLADO → RETRY → DLT
# ------------------------------------------------------------
demo_fallo() {
  echo ""
  echo "============================================================"
  echo "  ESCENARIO 2 — FALLO CONTROLADO (retry + DLT)"
  echo "============================================================"

  local ts; ts=$(date +%s)
  local dni; dni=$(printf "%08d" $(( (ts+1) % 100000000 )))

  log "Creando socio con email 'venenoso' (fail@...)..."
  local socio_id
  socio_id=$(crear_socio "Falla $ts" "fail+$ts@example.com" "$dni")
  log_ok "Socio: $socio_id"

  log "Creando clase futura..."
  local fecha; fecha=$(date -u -d "+2 days" +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null \
    || date -u -v+2d +"%Y-%m-%dT%H:%M:%SZ")
  local clase_id
  clase_id=$(crear_clase "Clase Fallo $ts" "Prof. Fallo" "$fecha")
  log_ok "Clase: $clase_id"

  log "Reservando (el consumer fallará a propósito)..."
  reservar "$socio_id" "$clase_id"
  log_ok "Reserva creada — ahora el consumer lanzará excepción"

  log "Esperando 15s para que se agoten los 3 retries (2+4+8s)..."
  for i in $(seq 15 -1 1); do
    printf "\r  ⏱  %2ds restantes..." "$i"
    sleep 1
  done
  echo ""

  log "Verificando tópicos de retry y DLT..."
  docker exec "$KAFKA_CONTAINER" kafka-topics \
    --bootstrap-server gym-kafka:29092 --list 2>/dev/null \
    | grep -E "gym\.reservas\.eventos\.v1-(retry|dlt)" \
    | sed 's/^/  · /' || log_warn "Los topics retry/dlt aún no existen"

  echo ""
  log "Verifica en Kafka UI:"
  echo "  → $KAFKA_UI/topics/gym.reservas.eventos.v1-dlt"
  echo "  → header: kafka_dlt-exception-message = Simulated mail failure"

  log "Verifica en logs de la API:"
  echo "  → docker compose logs --tail=50 gym-api | grep -E 'Simulando fallo|\\[DLT\\]'"
}

# ------------------------------------------------------------
# Escenario 3: CONTADOR KAFKA STREAMS
# ------------------------------------------------------------
demo_contador() {
  echo ""
  echo "============================================================"
  echo "  ESCENARIO 3 — CONTADOR KAFKA STREAMS"
  echo "============================================================"

  local ts; ts=$(date +%s)

  log "Creando clase única para el contador..."
  local fecha; fecha=$(date -u -d "+2 days" +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null \
    || date -u -v+2d +"%Y-%m-%dT%H:%M:%SZ")
  local clase_id
  clase_id=$(crear_clase "Contador Demo $ts" "Prof. Contador" "$fecha")
  log_ok "Clase: $clase_id"

  log "Creando 3 socios y reservando la misma clase..."
  local i
  for i in 1 2 3; do
    local dni; dni=$(printf "%08d" $(( (ts+i) % 100000000 )))
    local socio_id
    socio_id=$(crear_socio "Contador $i" "contador$i-$ts@example.com" "$dni")
    reservar "$socio_id" "$clase_id"
    log_ok "Reserva $i hecha (socio $socio_id)"
  done

  log "Esperando 2s para que Kafka Streams emita..."
  sleep 2

  log "Consultando tópico gym.reservas.contador.v1..."
  timeout 5 docker exec "$KAFKA_CONTAINER" kafka-console-consumer \
    --bootstrap-server gym-kafka:29092 \
    --topic gym.reservas.contador.v1 \
    --from-beginning \
    --property print.key=true 2>/dev/null \
    | grep "$clase_id" \
    | sed 's/^/  · key → /' || log_warn "No se encontró el contador todavía"

  echo ""
  log "Verifica en Kafka UI:"
  echo "  → $KAFKA_UI/topics/gym.reservas.contador.v1"
  echo "  → clase $clase_id debe aparecer con value=1,2,3"
}

# ------------------------------------------------------------
# Main
# ------------------------------------------------------------
main() {
  require_jq
  check_stack

  case "${1:-todo}" in
    feliz)    demo_feliz ;;
    fallo)    demo_fallo ;;
    contador) demo_contador ;;
    todo)
      demo_feliz
      demo_fallo
      demo_contador
      echo ""
      echo "============================================================"
      echo "  DEMO COMPLETA"
      echo "============================================================"
      log_ok "Todos los escenarios ejecutados"
      ;;
    *)
      echo "Uso: $0 [feliz|fallo|contador|todo]"
      exit 1
      ;;
  esac
}

main "$@"