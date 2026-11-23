package pe.edu.galaxy.training.java.sdd.gimnasio.application.service.notificacion;

import pe.edu.galaxy.training.java.sdd.gimnasio.domain.event.ReservaConfirmadaEvent;

/**
 * Puerto de aplicación para notificaciones al socio.
 * La implementación concreta (JavaMail) vive en infrastructure.
 */
public interface NotificacionService {

    void enviarConfirmacionReserva(ReservaConfirmadaEvent event);
}