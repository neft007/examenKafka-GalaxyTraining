package pe.edu.galaxy.training.java.sdd.gimnasio.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import pe.edu.galaxy.training.java.sdd.gimnasio.domain.event.ReservaConfirmadaEvent;

/**
 * Configuración del consumidor Kafka.
 * <p>
 * En Fase E se agregará aquí el DefaultErrorHandler y el retry/DLT.
 */
@Configuration
@EnableKafka
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers:gym-kafka:29092}")
    private String bootstrapServers;

    @Bean
    public ConsumerFactory<String, ReservaConfirmadaEvent> reservaConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "pe.edu.galaxy.training.java.sdd.gimnasio.domain.event");
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, ReservaConfirmadaEvent.class.getName());

        return new DefaultKafkaConsumerFactory<>(props,
                new StringDeserializer(),
                new JsonDeserializer<>(ReservaConfirmadaEvent.class, false));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, ReservaConfirmadaEvent> kafkaListenerContainerFactory(
            ConsumerFactory<String, ReservaConfirmadaEvent> reservaConsumerFactory) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, ReservaConfirmadaEvent>();
        factory.setConsumerFactory(reservaConsumerFactory);
        factory.setConcurrency(2); // 2 threads → consume 2 particiones en paralelo
        return factory;
    }
}