package pe.edu.galaxy.training.java.sdd.gimnasio.streams;

import java.util.HashMap;
import java.util.Map;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.apache.kafka.streams.StreamsConfig;
import pe.edu.galaxy.training.java.sdd.gimnasio.domain.event.avro.ReservaConfirmadaEvent;

/**
 * Topología de Kafka Streams que cuenta reservas confirmadas por clase.
 * <p>
 * Spring Boot gestiona el ciclo de vida del KafkaStreams a través de
 * @EnableKafkaStreams + StreamsBuilder inyectado. No creamos el KafkaStreams
 * manualmente para evitar problemas de arranque silencioso.
 */
@Configuration
@EnableKafkaStreams
public class ReservasContadorTopology {

    private static final Logger log = LoggerFactory.getLogger(ReservasContadorTopology.class);

    static final String TOPIC_INPUT = "gym.reservas.eventos.v1";
    static final String TOPIC_OUTPUT = "gym.reservas.contador.v1";

    @Value("${spring.kafka.properties.schema.registry.url:http://gym-schema-registry:8081}")
    private String schemaRegistryUrl;

    @Value("${spring.kafka.bootstrap-servers:gym-kafka:29092}")
    private String bootstrapServers;

    @Bean
    public KStream<String, ReservaConfirmadaEvent> reservasContadorStream(StreamsBuilder builder) {
        final SpecificAvroSerde<ReservaConfirmadaEvent> avroSerde = new SpecificAvroSerde<>();
        final Map<String, String> serdeConfig = new HashMap<>();
        serdeConfig.put("schema.registry.url", schemaRegistryUrl);
        serdeConfig.put("specific.avro.reader", "true");
        avroSerde.configure(serdeConfig, false);

        KStream<String, ReservaConfirmadaEvent> reservas =
                builder.stream(TOPIC_INPUT, Consumed.with(Serdes.String(), avroSerde));

        KTable<String, Long> contador = reservas
                .filter((k, v) -> v != null && "ReservaConfirmada".contentEquals(v.getEventType()))
                .groupBy((k, v) -> v.getClaseId().toString(), Grouped.with(Serdes.String(), avroSerde))
                .count();

        contador.toStream()
                .mapValues(Object::toString)
                .to(TOPIC_OUTPUT, Produced.with(Serdes.String(), Serdes.String()));

        log.info("[KAFKA-STREAMS] Topología registrada: {} → {}", TOPIC_INPUT, TOPIC_OUTPUT);
        return reservas;
    }

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "gym-reservas-contador-v2"); // ← CAMBIAR
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-gym-v2"); // ← CAMBIAR también
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        props.put("schema.registry.url", schemaRegistryUrl);

        // Ignorar mensajes viejos/corruptos
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "latest");
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                org.apache.kafka.streams.errors.LogAndContinueExceptionHandler.class);

        return new KafkaStreamsConfiguration(props);
    }
}