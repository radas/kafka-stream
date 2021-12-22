package com.nordea.kafka.convertor;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.springframework.stereotype.Component;

/**
 * @author Radek
 * @since 2021-12-21
 */
@Component
@RequiredArgsConstructor
public class JsonConvertor {

    private final ObjectMapper mapper;


    @SneakyThrows
    public <T> String toJson(final T object) {
        return mapper.writeValueAsString(object);

    }

    @SneakyThrows
    public <T> T fromJson(final String json, final Class<T> clazz) {
        return mapper.readValue(json, clazz);
    }


}
