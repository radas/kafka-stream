package com.nordea.kafka.transformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nordea.kafka.domain.Event;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.WindowStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static com.nordea.kafka.config.WindowStoreConfig.STORE_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 * @author Radek
 * @since 2021-12-21
 */
@SpringBootTest
public class DeduplicationTest {
    public static final String INPUT_TOPIC = "input-topic";
    public static final String OUTPUT_TOPIC = "deduplicated-topic";

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    private StoreBuilder<WindowStore<String, Long>> windowStore;

    @Autowired
    private Transformer<String, String, KeyValue<String, String>> transformer;

    private TopologyTestDriver testDriver;

    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, String> outputTopic;

    @BeforeEach
    void beforeEach() throws Exception {
        // Create topology to handle stream of users
        final StreamsBuilder builder = new StreamsBuilder();

        // Dummy properties needed for test diver
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        builder.addStateStore(windowStore);


        final KStream<String, String> stream = builder.stream(INPUT_TOPIC);

        final KStream<String, String> deduplicated = stream.transform(() -> transformer,
                STORE_NAME);

        deduplicated.to(OUTPUT_TOPIC);

        // Create test driver
        final Topology topology = builder.build();
        testDriver = new TopologyTestDriver(topology, props);


        // Define input and output topics to use in tests
        inputTopic = testDriver.createInputTopic(
                INPUT_TOPIC,
                new StringSerializer(),
                new StringSerializer());

        outputTopic = testDriver.createOutputTopic(
                OUTPUT_TOPIC,
                new StringDeserializer(),
                new StringDeserializer());


    }

    @AfterEach
    void afterEach() {
        testDriver.close();
    }

    @Test
    void shouldPropagateUserWithFavoriteColorRed() throws Exception {
        final Event event = new Event();
        event.setJournalId(String.valueOf(10));
        event.setType("KEY");

        final Event event2 = new Event();
        event2.setJournalId(String.valueOf(11));
        event2.setType("KEY2");

        final Event event3 = new Event();
        event3.setJournalId(String.valueOf(10));
        event3.setType("KEY3");

        final String value1 = objectMapper.writeValueAsString(event);
        final String value2 = objectMapper.writeValueAsString(event2);
        final String value3 = objectMapper.writeValueAsString(event3);

        final List<String> inputValues = Arrays.asList(value1, value2, value3);
        final List<String> expectedValues = Arrays.asList(value1, value2);


        inputTopic.pipeValueList(inputValues);

        final List<String> deduplicated = outputTopic.readValuesToList();

        assertThat(deduplicated, equalTo(expectedValues));
    }

}