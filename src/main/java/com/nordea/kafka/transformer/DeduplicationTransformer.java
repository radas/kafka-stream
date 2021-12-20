package com.nordea.kafka.transformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nordea.kafka.domain.Event;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import static com.nordea.kafka.stream.KafkaStream.STORE_NAME;

/**
 * @author Radek
 * @since 2021-12-20
 */
@Slf4j
@RequiredArgsConstructor
public class DeduplicationTransformer<K, V, E> implements Transformer<K, V, KeyValue<K, V>> {

    private ProcessorContext context;
    //autowired
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Key: event ID
     * Value: timestamp (event-time) of the corresponding event when the event ID was seen for the
     * first time
     */
    private WindowStore<String, Long> eventIdStore;

    private final long leftDurationMs;
    private final long rightDurationMs;

    private final KeyValueMapper<K, V, E> idExtractor;

    /**
     * @param maintainDurationPerEventInMs how long to "remember" a known event (or rather, an event
     *                                     ID), during the time of which any incoming duplicates of
     *                                     the event will be dropped, thereby de-duplicating the
     *                                     input.
     * @param idExtractor                  extracts a unique identifier from a record by which we de-duplicate input
     *                                     records; if it returns null, the record will not be considered for
     *                                     de-duping but forwarded as-is.
     */
    public DeduplicationTransformer(final long maintainDurationPerEventInMs, final KeyValueMapper<K, V, E> idExtractor) {
        if (maintainDurationPerEventInMs < 1) {
            throw new IllegalArgumentException("maintain duration per event must be >= 1");
        }
        leftDurationMs = maintainDurationPerEventInMs / 2;
        rightDurationMs = maintainDurationPerEventInMs - leftDurationMs;
        this.idExtractor = idExtractor;

        log.info("LeftDurations: {} rightDuration {} idExtractor: {}", leftDurationMs / 1000, rightDurationMs / 1000, idExtractor);
    }

    @Override
    public void init(final ProcessorContext context) {
        this.context = context;
        eventIdStore = (WindowStore<String, Long>) context.getStateStore(STORE_NAME);
    }


    @SneakyThrows
    @Override
    public KeyValue<K, V> transform(final K key, final V value) {
        log.info("Key {}, Value: {}", key, value);
        final String eventId = (String) idExtractor.apply(key, value);

        final Event event = objectMapper.readValue(eventId, Event.class);

        final String number = String.valueOf(event.getNumber());

        log.info("EVENT ID: {}", number);

        if (eventId.isBlank()) {
            return KeyValue.pair(key, value);
        } else {
            final KeyValue<K, V> output;
            if (isDuplicate(number)) {
                log.info("Is duplicate!!!!");
                output = null;
                updateTimestampOfExistingEventToPreventExpiry(number, context.timestamp());
            } else {
                output = KeyValue.pair(key, value);
                rememberNewEvent(number, context.timestamp());
            }
            return output;
        }
    }

    private boolean isDuplicate(final String eventId) {
        final long eventTime = context.timestamp();
        final WindowStoreIterator<Long> timeIterator = eventIdStore.fetch(
                String.valueOf(eventId),
                eventTime - leftDurationMs,
                eventTime + rightDurationMs);
        final boolean isDuplicate = timeIterator.hasNext();
        timeIterator.close();
        return isDuplicate;
    }

    private void updateTimestampOfExistingEventToPreventExpiry(final String eventId, final long newTimestamp) {
        eventIdStore.put(eventId, newTimestamp, newTimestamp);
    }

    private void rememberNewEvent(final String eventId, final long timestamp) {
        eventIdStore.put(eventId, timestamp, timestamp);
    }

    @Override
    public void close() {
        // Note: The store should NOT be closed manually here via `eventIdStore.close()`!
        // The Kafka Streams API will automatically close stores when necessary.

    }
}
