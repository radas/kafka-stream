package com.nordea.kafka.producer;

import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Random;

/**
 * @author Radek
 * @since 2021-12-20
 */
@Component
@RequiredArgsConstructor
public class NumberProducer {

    private final NumberPublisher numberPublisher;

    @Scheduled(fixedRate = 2000)
    public void produceIntStream() {
        final Random random = new Random();
        numberPublisher.produceRandomNumber(random.nextInt(10));
    }
}
