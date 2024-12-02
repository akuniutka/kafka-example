package io.github.akuniutka.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.kafka.common.serialization.Serializer;

public class WeatherEventJsonSerializer implements Serializer<WeatherEvent> {

    private final JsonMapper jsonMapper = new JsonMapper();

    @Override
    public byte[] serialize(final String topic, final WeatherEvent data) {
        try {
            return jsonMapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
