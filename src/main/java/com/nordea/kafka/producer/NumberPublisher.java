package com.nordea.kafka.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nordea.kafka.domain.Event;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.List;

import static com.nordea.kafka.stream.KafkaStream.INPUT_TOPIC_NAME;

/**
 * @author Radek
 * @since 2021-12-20
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class NumberPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @SneakyThrows
    public void produceRandomNumber(final Integer randomNumber) {

        String key = "Odd";
        if (randomNumber % 2 == 0) key = "Even";

        final Event event = new Event();
        event.setNumber(randomNumber);
        event.setType(key);
        
        final String value = objectMapper.writeValueAsString(event);

        log.info("=========== START ===========");
        log.info("Producing key: {} and value: {}", key, value);


        final ProducerRecord<String, String> producerRecord = buildProducerRecord(key, value, INPUT_TOPIC_NAME);

        final ListenableFuture<SendResult<String, String>> listenableFuture = kafkaTemplate.send(producerRecord);

        final String finalKey = key;

        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
            @Override
            public void onFailure(final Throwable ex) {
                handleFailure(finalKey, value, ex);
            }

            @Override
            public void onSuccess(final SendResult<String, String> result) {
                handleSuccess(finalKey, value, result);
            }
        });

    }

    private ProducerRecord<String, String> buildProducerRecord(final String key, final String value, final String topic) {


        final List<Header> recordHeaders = List.of(new RecordHeader("my_header", key.getBytes()));

        return new ProducerRecord<>(topic, null, key, value, recordHeaders);
    }

    private void handleFailure(final String key, final String value, final Throwable ex) {
        log.error("Error Sending the Message and the exception is {}", ex.getMessage());
        try {
            throw ex;
        } catch (final Throwable throwable) {
            log.error("Error in OnFailure: {}", throwable.getMessage());
        }


    }

    private void handleSuccess(final String key, final String value, final SendResult<String, String> result) {
        log.info("Message Sent SuccessFully for the key : {} and the value is {} , partition is {}", key, value, result.getRecordMetadata().partition());
    }
}
