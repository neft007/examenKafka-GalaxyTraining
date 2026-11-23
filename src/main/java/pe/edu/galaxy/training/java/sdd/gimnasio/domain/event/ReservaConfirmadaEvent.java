package pe.edu.galaxy.training.java.sdd.gimnasio.domain.event;

import java.time.Instant;
import java.util.UUID;

/*
 * El campo {@code email} viaja aquí para que el consumer de notificaciones
 * no necesite consultar la BD para saber a quién notificar.
 */
public record ReservaConfirmadaEvent(
        UUID eventId,
        String eventType,
        UUID reservaId,
        UUID socioId,
        UUID claseId,
        String socioEmail,
        Instant occurredAt
) {
    public static ReservaConfirmadaEvent of(UUID reservaId,
                                            UUID socioId,
                                            UUID claseId,
                                            String socioEmail,
                                            Instant occurredAt) {
        return new ReservaConfirmadaEvent(
                UUID.randomUUID(),
                "ReservaConfirmada",
                reservaId,
                socioId,
                claseId,
                socioEmail,
                occurredAt);
    }
}