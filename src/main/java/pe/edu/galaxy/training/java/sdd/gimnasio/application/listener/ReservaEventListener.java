package pe.edu.galaxy.training.java.sdd.gimnasio.application.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

import pe.edu.galaxy.training.java.sdd.gimnasio.application.port.out.EventPublisherPort;
import pe.edu.galaxy.training.java.sdd.gimnasio.domain.event.ReservaConfirmadaEvent;

/**
 * Listener de Spring Events que desacopla la capa de servicio del productor Kafka.
 * <p>
 * Se dispara SOLO cuando la transacción del servicio ha hecho commit (AFTER_COMMIT).
 * Si la transacción hace rollback, el evento NO se publica en Kafka (consistencia).
 * <p>
 */
@Component
public class ReservaEventListener {

    private static final Logger log = LoggerFactory.getLogger(ReservaEventListener.class);

    static final String TOPIC_RESERVAS = "gym.reservas.eventos.v1";

    private final EventPublisherPort eventPublisher;

    public ReservaEventListener(EventPublisherPort eventPublisher) {
        this.eventPublisher = eventPublisher;
    }

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void onReservaConfirmada(ReservaConfirmadaEvent event) {
        log.info("[SPRING-EVENT] ReservaConfirmada recibido reservaId={} socioId={}",
                event.reservaId(), event.socioId());
        // key = reservaId → garantiza orden por reserva dentro de la partición
        eventPublisher.publish(TOPIC_RESERVAS, event.reservaId().toString(), event);
    }
}