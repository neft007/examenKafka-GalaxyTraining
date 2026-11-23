package pe.edu.galaxy.training.java.sdd.gimnasio.adapter.in.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import pe.edu.galaxy.training.java.sdd.gimnasio.application.service.notificacion.NotificacionService;
import pe.edu.galaxy.training.java.sdd.gimnasio.domain.event.ReservaConfirmadaEvent;

/**
 * Consumidor Kafka del tópico de reservas con tolerancia a fallos.
 * <p>
 * @RetryableTopic crea automáticamente:
 *   - gym.reservas.eventos.v1-retry-0  (espera 2s)
 *   - gym.reservas.eventos.v1-retry-1  (espera 4s)
 *   - gym.reservas.eventos.v1-retry-2  (espera 8s)
 *   - gym.reservas.eventos.v1-dlt      (destino final si los 3 retries fallan)
 * <p>
 * @DltHandler recibe el mensaje que agotó los reintentos.
 * Headers de retry (auto-inyectados por Spring):
 *   - kafka_original-topic
 *   - kafka_original-partition
 *   - kafka_original-offset
 *   - kafka_original-timestamp
 *   - kafka_dlt-exception-message
 */
@Component
public class ReservaNotificationConsumer {

    private static final Logger log = LoggerFactory.getLogger(ReservaNotificationConsumer.class);

    private final NotificacionService notificacionService;

    public ReservaNotificationConsumer(NotificacionService notificacionService) {
        this.notificacionService = notificacionService;
    }

    @RetryableTopic(
            attempts = "4",                                  // 1 original + 3 retries
            // ✅ Correcto
            backOff = @BackOff(delay = 2000, multiplier = 2.0, maxDelay = 10000),
            autoCreateTopics = "true",
            topicSuffixingStrategy = org.springframework.kafka.retrytopic.TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
            dltStrategy = DltStrategy.FAIL_ON_ERROR,
            include = { RuntimeException.class }              // solo reintenta excepciones recuperables
    )
    @KafkaListener(
            topics = "gym.reservas.eventos.v1",
            groupId = "gym-notificaciones",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onReservaConfirmada(
            @Payload ReservaConfirmadaEvent event,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(name = "correlationId", required = false) byte[] correlationIdHeader) {

        String correlationId = correlationIdHeader == null ? "-" : new String(correlationIdHeader);
        log.info("[KAFKA-CONSUMER] Recibido partition={} offset={} key={} correlationId={}",
                partition, offset, key, correlationId);
        log.info("[KAFKA-CONSUMER] eventType={} reservaId={} socioEmail={}",
                event.eventType(), event.reservaId(), event.socioEmail());

        // Si esto lanza excepción, @RetryableTopic toma el control
        notificacionService.enviarConfirmacionReserva(event);
    }

    /**
     * Se invoca cuando el mensaje agotó los reintentos y llegó a la DLT.
     * Aquí tambien podemos:
     *   - Guardar en una tabla de "eventos fallidos" para revisión manual
     *   - Enviar alerta a Slack/email de operaciones
     *   - Exponer un endpoint para reprocesar la DLT
     */
    @DltHandler
    public void handleDlt(
            @Payload ReservaConfirmadaEvent event,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(name = KafkaHeaders.DLT_EXCEPTION_MESSAGE, required = false) String exceptionMessage,
            @Header(name = KafkaHeaders.DLT_ORIGINAL_TOPIC, required = false) byte[] originalTopic,
            @Header(name = KafkaHeaders.DLT_ORIGINAL_OFFSET, required = false) byte[] originalOffset) {

        log.error("[DLT] Mensaje agotó reintentos y llegó a DLT. topic={} reservaId={} socioEmail={} exception={} originalTopic={} originalOffset={}",
                topic, event.reservaId(), event.socioEmail(), exceptionMessage,
                originalTopic == null ? "-" : new String(originalTopic),
                originalOffset == null ? "-" : new String(originalOffset));

        // TODO: persistir en tabla dlt_eventos_fallidos para reprocesamiento
    }
}