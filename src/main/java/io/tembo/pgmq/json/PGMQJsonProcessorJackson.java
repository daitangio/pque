package io.tembo.pgmq.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.tembo.pgmq.PGMQException;
import org.springframework.util.Assert;

public class PGMQJsonProcessorJackson implements PGMQJsonProcessor {

    private final ObjectMapper objectMapper;

    public PGMQJsonProcessorJackson(ObjectMapper objectMapper) {
        Assert.notNull(objectMapper, "ObjectMapper must not be null!");

        this.objectMapper = objectMapper;
    }

    @Override
    public boolean isJson(String json) {
        try {
            objectMapper.readTree(json);
        } catch (JsonProcessingException e) {
            return false;
        }

        return true;
    }

    @Override
    public String toJson(Object object) {
        try {
            return objectMapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new PGMQException("Failed to serialize object to JSON string", e);
        }
    }

    @Override
    public <T> T fromJson(String json, Class<T> toClazz) {
        try {
            return objectMapper.readValue(json, toClazz);
        } catch (JsonProcessingException e) {
            throw new PGMQException("Failed to deserialize from JSON string to object", e);
        }
    }
}
