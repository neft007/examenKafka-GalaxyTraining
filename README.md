# Sistema de Reservas para Gimnasio · Apache Kafka

Proyecto final del curso **Kafka for Java Developers** (Galaxy Training).

Sistema de reservas de gimnasio con arquitectura hexagonal (Spring Boot 4 + Java 17) y **mensajería asíncrona con Apache Kafka**. Cubre los 10 componentes de la rúbrica: arquitectura de integración, infraestructura, CLI, producer, consumer, Spring Events, error handlers (producer + consumer), integración con JavaMail y extras (Schema Registry + Avro + Kafka Streams).

---

## 📚 Stack

| Capa | Tecnología |
|------|-----------|
| Backend | Spring Boot 4.1.1 · Java 17 · Spring Kafka · Spring Data JPA |
| Frontend | Nginx · HTML/CSS/JS |
| Base de datos | PostgreSQL 16 |
| Mensajería | Apache Kafka 7.7 (KRaft) · Schema Registry (Confluent 7.7) |
| Serialización | Avro (con schema versionado) |
| Streaming | Kafka Streams (contador de reservas por clase) |
| Mail | JavaMail + MailHog (SMTP local) |
| Contenedores | Docker · Docker Compose |
| Docs API | OpenAPI 3.0.3 · Swagger UI |

---

## 🚀 Cómo levantar el proyecto

### Requisitos
- Docker ≥ 24 y Docker Compose ≥ 2.20
- 4 GB de RAM libres

### Pasos

```bash
# 1. Clonar el repositorio
git clone <URL_DEL_REPO>
cd gimnasio-reservas_docker

# 2. Crear el archivo de password para Postgres
cp db_password.txt.example db_password.txt
# (el contenido por defecto es "postgres", puedes cambiarlo)

# 3. Levantar todos los servicios
docker compose up -d --build

# 4. Esperar ~60s a que la API arranque
docker compose logs -f gym-api
```

### URLs de los servicios

| Servicio | URL | Credenciales |
|----------|-----|--------------|
| Frontend | http://localhost:3000 | — |
| API REST | http://localhost:8080/api/v1 | — |
| Swagger UI | http://localhost:8080/swagger-ui/index.html | — |
| Actuator | http://localhost:8080/actuator/health | — |
| Kafka UI | http://localhost:8090 | — |
| Schema Registry | http://localhost:8081/subjects | — |
| MailHog (bandeja) | http://localhost:8025 | — |
| MailHog SMTP | localhost:1025 | — |
| Adminer (DB) | http://localhost:8081 | server: `gym-db`, user: `postgres`, db: `gimnasiodb` |
| PostgreSQL | localhost:5432 | user: `postgres`, password: ver `db_password.txt` |

---

## 🏗️ Arquitectura

Los diagramas completos están en [`docs/diagramas/`](docs/diagramas/) (PlantUML) y [`docs/diagramas-mermaid.md`](docs/diagramas-mermaid.md) (Mermaid renderizable en GitHub).

Vista general:

```
┌────────────┐   ┌──────────────────────────────────────┐   ┌─────────────────────┐
│  Frontend  │──▶│  gym-api (Spring Boot 4)             │──▶│ PostgreSQL          │
│  (Nginx)   │   │                                      │   └─────────────────────┘
└────────────┘   │  Controller → Service(@Transactional)│
                 │       │                              │
                 │       ▼                              │
                 │  ApplicationEventPublisher           │
                 │       │                              │
                 │       ▼ (AFTER_COMMIT)               │
                 │  KafkaProducerAdapter (Avro)         │
                 └───────────────┬──────────────────────┘
                                 │
                                 ▼
                 ┌──────────────────────────────────────┐
                 │  Apache Kafka (KRaft) + Schema Reg.  │
                 │  · gym.reservas.eventos.v1           │
                 │  · gym.reservas.eventos.v1-retry-0..2│
                 │  · gym.reservas.eventos.v1-dlt       │
                 │  · gym.reservas.contador.v1          │
                 └───────────────┬──────────────────────┘
                    ┌────────────┴─────────────┐
                    ▼                          ▼
        ┌────────────────────┐      ┌──────────────────────┐
        │ ReservaNotification│      │ Kafka Streams        │
        │ Consumer           │      │ (contador por clase) │
        │ @RetryableTopic    │      └──────────────────────┘
        │ @DltHandler        │
        └─────────┬──────────┘
                  ▼
        ┌────────────────────┐
        │ JavaMail → MailHog │
        └────────────────────┘
```

---

## ✅ Cobertura de la rúbrica

| # | Componente | Dónde está implementado |
|---|-----------|-------------------------|
| 1 | Arquitectura de integración | [`docs/diagramas/01-arquitectura-general.puml`](docs/diagramas/01-arquitectura-general.puml) |
| 2 | Infraestructura + Kafka UI | `docker-compose.yml` (servicios `kafka`, `kafka-ui`, `schema-registry`, `mailhog`) |
| 3 | Kafka CLI (topics, produce, consume, 2 groups) | [`scripts/kafka-cli-demo.sh`](scripts/kafka-cli-demo.sh) |
| 4 | Producer Avro con Key + headers | `adapter/out/messaging/KafkaProducerAdapter.java` |
| 5 | Consumer Avro | `adapter/in/messaging/ReservaNotificationConsumer.java` |
| 6 | Spring Events (desacople servicio ↔ producer) | `ReservarPlazaService` + `ReservaEventListener` |
| 7 | Error handler Producer (acks, retries, idempotence, callback) | `config/KafkaAvroProducerConfig.java` + `KafkaProducerAdapter` |
| 8 | Error handler Consumer (@RetryableTopic + @DltHandler) | `ReservaNotificationConsumer` |
| 9 | Integración JavaMail → MailHog | `adapter/out/email/JavaMailNotificacionAdapter.java` |
| 10 | Extras: Schema Registry + Avro + Kafka Streams | `config/KafkaAvroProducerConfig`, `src/main/avro/*.avsc`, `streams/ReservasContadorTopology` |

---

## 🧪 Demo rápida (para la presentación)

### A) Reserva exitosa → correo en MailHog

1. Abrir http://localhost:3000
2. Registrar socio (email normal, ej. `juan@example.com`)
3. Crear clase con fecha futura
4. Reservar plaza
5. Verificar:
   - http://localhost:8025 → correo recibido
   - http://localhost:8090/topics/gym.reservas.eventos.v1 → mensaje Avro
   - http://localhost:8081/subjects → `gym.reservas.eventos.v1-value` (v1)

### B) Fallo controlado → retry → DLT

1. Registrar socio con email `fail@example.com`
2. Reservar con ese socio
3. Esperar ~15s (backoff 2+4+8)
4. En Kafka UI aparecerán los tópicos `gym.reservas.eventos.v1-retry-0`, `-retry-1`, `-retry-2` y `-dlt`
5. El `@DltHandler` imprime en logs:
   ```
   [DLT] Mensaje agotó reintentos. originalTopic=gym.reservas.eventos.v1
   exception=Simulated mail failure for email: fail@example.com
   ```

### C) Contador de Kafka Streams

1. Hacer 3 reservas en la **misma clase**
2. En Kafka UI → `gym.reservas.contador.v1`
3. Ver mensajes con `key=<claseId>` y `value=1, 2, 3`

---

## 🛠️ Comandos útiles

### Kafka CLI

```bash
# Menú interactivo
./scripts/kafka-cli-demo.sh

# Crear topics
./scripts/kafka-cli-topics.sh create

# Producir desde archivo
./scripts/kafka-cli-produce.sh file
./scripts/kafka-cli-produce.sh json

# Consumer grupo A (gym-notificaciones)
./scripts/kafka-cli-consume.sh group-a

# Consumer grupo B (gym-auditoria)
./scripts/kafka-cli-consume.sh group-b

# Listar grupos
./scripts/kafka-cli-consume.sh list-groups
```

### Verificar Schema Registry

```bash
curl http://localhost:8081/subjects
# ["gym.reservas.eventos.v1-value"]

curl http://localhost:8081/subjects/gym.reservas.eventos.v1-value/versions/1 | jq .
```

### Ver la DLT

```bash
docker exec -it gym-kafka kafka-console-consumer \
  --bootstrap-server gym-kafka:29092 \
  --topic gym.reservas.eventos.v1-dlt \
  --from-beginning \
  --property print.headers=true
```

---

## 📂 Estructura del proyecto

```
gimnasio-reservas_docker/
├── docker-compose.yml
├── Dockerfile
├── pom.xml
├── db_password.txt.example
├── docs/
│   ├── diagramas/*.puml
│   └── diagramas-mermaid.md
├── scripts/
│   ├── kafka-cli-demo.sh
│   ├── kafka-cli-topics.sh
│   ├── kafka-cli-produce.sh
│   ├── kafka-cli-consume.sh
│   └── data/*.{txt,json}
├── frontend/
└── src/main/
    ├── avro/ReservaConfirmadaEvent.avsc
    ├── java/pe/edu/galaxy/training/java/sdd/gimnasio/
    │   ├── adapter/
    │   │   ├── in/messaging/ReservaNotificationConsumer.java
    │   │   ├── in/web/…
    │   │   ├── out/email/JavaMailNotificacionAdapter.java
    │   │   ├── out/messaging/KafkaProducerAdapter.java
    │   │   └── out/persistence/…
    │   ├── application/…
    │   ├── config/…
    │   │   ├── KafkaAvroProducerConfig.java
    │   │   ├── KafkaConsumerConfig.java
    │   │   └── …
    │   ├── domain/…
    │   └── streams/ReservasContadorTopology.java
    └── resources/application.properties
```

---

## 👨‍💻 Autor

**Neftali.** — Curso *Kafka for Java Developers* · Galaxy Training
Instructor: Aristides Novoa Arbildo