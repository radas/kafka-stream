package com.nordea.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import static com.nordea.kafka.stream.KafkaStream.OUTPUT_TOPIC_NAME;

/**
 * @author Radek
 * @since 2021-12-20
 */
@Component
@EnableKafka
@Slf4j
public class EvenNumberConsumer {

    @KafkaListener(topics = OUTPUT_TOPIC_NAME)
    public void receive(final ConsumerRecord<String, String> record) {
        log.info("Received number: {}", record.value());
        log.info("Received header: {}", record.headers().toString());

        log.info("=========== END ===========");
    }

}
