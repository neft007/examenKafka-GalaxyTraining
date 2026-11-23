package pe.edu.galaxy.training.java.sdd.gimnasio.adapter.in.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import pe.edu.galaxy.training.java.sdd.gimnasio.application.service.notificacion.NotificacionService;
import pe.edu.galaxy.training.java.sdd.gimnasio.domain.event.ReservaConfirmadaEvent;

/**
 * Consumidor Kafka del tópico de reservas.
 * <p>
 * Group ID "gym-notificaciones" coincide con el grupo usado en la CLI,
 * lo cual demuestra que el consumer Java y los consumers de CLI comparten semántica.
 * <p>
 *   - @RetryableTopic (reintentos exponenciales)
 *   - @DltHandler (Dead Letter Topic)
 *   - uso de @Header(MessageHeaders) para trazabilidad
 */
@Component
public class ReservaNotificationConsumer {

    private static final Logger log = LoggerFactory.getLogger(ReservaNotificationConsumer.class);

    private final NotificacionService notificacionService;

    public ReservaNotificationConsumer(NotificacionService notificacionService) {
        this.notificacionService = notificacionService;
    }

    @KafkaListener(
            topics = "gym.reservas.eventos.v1",
            groupId = "gym-notificaciones",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onReservaConfirmada(
            @Payload ReservaConfirmadaEvent event,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        log.info("[KAFKA-CONSUMER] Recibido topic=gym.reservas.eventos.v1 partition={} offset={} key={}",
                partition, offset, key);
        log.info("[KAFKA-CONSUMER] eventType={} reservaId={} socioEmail={}",
                event.eventType(), event.reservaId(), event.socioEmail());

        notificacionService.enviarConfirmacionReserva(event);
    }
}