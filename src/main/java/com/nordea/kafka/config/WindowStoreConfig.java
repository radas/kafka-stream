package com.nordea.kafka.config;

import com.nordea.kafka.transformer.DeduplicationTransformer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

/**
 * @author Radek
 * @since 2021-12-21
 */
@Configuration
@Slf4j
public class WindowStoreConfig {
    // How long we "remember" an event.  During this time, any incoming duplicates of the event
    // will be, well, dropped, thereby de-duplicating the input data.
    //
    // The actual value depends on your use case.  To reduce memory and disk usage, you could
    // decrease the size to purge old windows more frequently at the cost of potentially missing out
    // on de-duplicating late-arriving records.
    public static final Duration windowSize = Duration.ofMinutes(1);

    // retention period must be at least window size -- for this use case, we don't need a longer retention period
    // and thus just use the window size as retention time
    final Duration retentionPeriod = windowSize;

    public static final String STORE_NAME = "windowStore";

    @Bean
    public StoreBuilder<WindowStore<String, Long>> createWindowStore() {
        log.info("CREATING STORE");
        return Stores.windowStoreBuilder(
                Stores.persistentWindowStore(STORE_NAME,
                        retentionPeriod,
                        windowSize,
                        false
                ),
                Serdes.String(),
                Serdes.Long());

    }

    @Bean
    public Transformer<String, String, KeyValue<String, String>> createTransformer() {
        log.info("Create transformer");
        return new DeduplicationTransformer<>(WindowStoreConfig.windowSize.toMillis(), (key, value) -> value);
    }

}
