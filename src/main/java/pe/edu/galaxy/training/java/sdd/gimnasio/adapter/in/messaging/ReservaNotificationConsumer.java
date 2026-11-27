package pe.edu.galaxy.training.java.sdd.gimnasio.adapter.in.messaging;

import java.time.Instant;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import pe.edu.galaxy.training.java.sdd.gimnasio.application.service.notificacion.NotificacionService;
import pe.edu.galaxy.training.java.sdd.gimnasio.domain.event.ReservaConfirmadaEvent;

/**
 * Consumidor Kafka del tópico de reservas con Avro + reintentos + DLT.
 */
@Component
public class ReservaNotificationConsumer {

    private static final Logger log = LoggerFactory.getLogger(ReservaNotificationConsumer.class);

    private final NotificacionService notificacionService;

    public ReservaNotificationConsumer(NotificacionService notificacionService) {
        this.notificacionService = notificacionService;
    }

    @RetryableTopic(
            attempts = "4",
            backOff = @BackOff(delay = 2000, multiplier = 2.0, maxDelay = 10000),
            autoCreateTopics = "true",
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
            dltStrategy = DltStrategy.FAIL_ON_ERROR,
            include = { RuntimeException.class }
    )
    @KafkaListener(
            topics = "gym.reservas.eventos.v1",
            groupId = "gym-notificaciones",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onReservaConfirmada(
            @Payload pe.edu.galaxy.training.java.sdd.gimnasio.domain.event.avro.ReservaConfirmadaEvent avroEvent,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(name = "correlationId", required = false) byte[] correlationIdHeader) {

        String correlationId = correlationIdHeader == null ? "-" : new String(correlationIdHeader);
        log.info("[KAFKA-CONSUMER] Recibido partition={} offset={} key={} correlationId={}",
                partition, offset, key, correlationId);

        // Convertir Avro → record Java de dominio (para no acoplar NotificacionService a Avro)
        ReservaConfirmadaEvent event = ReservaConfirmadaEvent.of(
                UUID.fromString(avroEvent.getReservaId().toString()),
                UUID.fromString(avroEvent.getSocioId().toString()),
                UUID.fromString(avroEvent.getClaseId().toString()),
                avroEvent.getSocioEmail().toString(),
                avroEvent.getOccurredAt());

        log.info("[KAFKA-CONSUMER] eventType={} reservaId={} socioEmail={}",
                event.eventType(), event.reservaId(), event.socioEmail());

        notificacionService.enviarConfirmacionReserva(event);
    }

    @DltHandler
    public void handleDlt(
            @Payload pe.edu.galaxy.training.java.sdd.gimnasio.domain.event.avro.ReservaConfirmadaEvent avroEvent,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(name = KafkaHeaders.DLT_EXCEPTION_MESSAGE, required = false) String exceptionMessage,
            @Header(name = KafkaHeaders.DLT_ORIGINAL_TOPIC, required = false) byte[] originalTopic,
            @Header(name = KafkaHeaders.DLT_ORIGINAL_OFFSET, required = false) byte[] originalOffset) {

        log.error("[DLT] Mensaje agotó reintentos. topic={} reservaId={} socioEmail={} exception={} originalTopic={} originalOffset={}",
                topic, avroEvent.getReservaId().toString(), avroEvent.getSocioEmail().toString(), exceptionMessage,
                originalTopic == null ? "-" : new String(originalTopic),
                originalOffset == null ? "-" : new String(originalOffset));
    }
}