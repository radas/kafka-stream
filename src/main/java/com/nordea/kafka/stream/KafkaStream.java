package com.nordea.kafka.stream;

import com.nordea.kafka.transformer.DeduplicationTransformer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

import java.time.Duration;

/**
 * @author Radek
 * @since 2021-12-20
 */

@Configuration
@EnableKafkaStreams
public class KafkaStream {

    public static final String OUTPUT_TOPIC_NAME = "even-number-topic";
    public static final String INPUT_TOPIC_NAME = "number-topic";
    public static final String STORE_NAME = "windowStore";

    @Bean
    public KStream<String, String> evenNumbersStream(final StreamsBuilder kStreamBuilder) {
        // How long we "remember" an event.  During this time, any incoming duplicates of the event
        // will be, well, dropped, thereby de-duplicating the input data.
        //
        // The actual value depends on your use case.  To reduce memory and disk usage, you could
        // decrease the size to purge old windows more frequently at the cost of potentially missing out
        // on de-duplicating late-arriving records.
        final Duration windowSize = Duration.ofMinutes(1);

        // retention period must be at least window size -- for this use case, we don't need a longer retention period
        // and thus just use the window size as retention time
        final Duration retentionPeriod = windowSize;

        final StoreBuilder<WindowStore<String, Long>> stateStore = Stores.windowStoreBuilder(
                Stores.persistentWindowStore(STORE_NAME,
                        retentionPeriod,
                        windowSize,
                        false
                ),
                Serdes.String(),
                Serdes.Long());


        kStreamBuilder.addStateStore(stateStore);

        final KStream<String, String> input = kStreamBuilder.stream(INPUT_TOPIC_NAME);

        final KStream<String, String> deduplicated = input.transform(() -> new DeduplicationTransformer<>(windowSize.toMillis(), (key, value) -> value),
                STORE_NAME);

        deduplicated.to(OUTPUT_TOPIC_NAME);

        return deduplicated;
    }
}
