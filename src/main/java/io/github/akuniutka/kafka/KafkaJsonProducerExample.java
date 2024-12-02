package io.github.akuniutka.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaJsonProducerExample {

    public static void main(final String[] args) {
        final Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.VoidSerializer");
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.github.akuniutka.kafka.WeatherEventJsonSerializer");

        try (Producer<Void, WeatherEvent> producer = new KafkaProducer<>(config)) {
            final String topic = "weather-events";
            final WeatherEvent message = new WeatherEvent();
            message.setTemperature(33.5);
            message.setLatitude(6.1944);
            message.setLongitude(106.8229);
            final ProducerRecord<Void, WeatherEvent> record = new ProducerRecord<>(topic, message);
            producer.send(record);
        }
    }
}
