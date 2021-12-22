package com.nordea.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import static com.nordea.kafka.processor.KafkaStreamProcessor.OUTPUT_TOPIC_NAME;

/**
 * @author Radek
 * @since 2021-12-20
 */
@Component
@EnableKafka
@Slf4j
public class EvenNumberConsumer {

    @KafkaListener(topics = OUTPUT_TOPIC_NAME)
    public void receive(final ConsumerRecord<String, String> eventRecord) {
        log.info("Received number: {}", eventRecord.value());
        log.info("Received header: {}", eventRecord.headers().toString());

        log.info("=========== END ===========");
    }

}
