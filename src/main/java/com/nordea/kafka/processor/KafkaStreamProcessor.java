package com.nordea.kafka.processor;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.stereotype.Component;

import static com.nordea.kafka.config.WindowStoreConfig.STORE_NAME;

/**
 * @author Radek
 * @since 2021-12-20
 */

@Component
@EnableKafkaStreams
@RequiredArgsConstructor
public class KafkaStreamProcessor {

    public static final String OUTPUT_TOPIC_NAME = "even-number-topic";
    public static final String INPUT_TOPIC_NAME = "number-topic";

    private final StoreBuilder<WindowStore<String, Long>> windowStore;

    private final Transformer<String, String, KeyValue<String, String>> transformer;


    @Bean
    public KStream<String, String> evenNumbersStream(final StreamsBuilder kStreamBuilder) {


        kStreamBuilder.addStateStore(windowStore);

        final KStream<String, String> input = kStreamBuilder.stream(INPUT_TOPIC_NAME);

        final KStream<String, String> deduplicated = input.transform(() -> transformer,
                STORE_NAME);

        deduplicated.to(OUTPUT_TOPIC_NAME);

        return deduplicated;
    }
}
