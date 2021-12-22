package com.nordea.kafka.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Random;

/**
 * @author Radek
 * @since 2021-12-20
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class NumberProducer {

    private final NumberPublisher numberPublisher;
    final Random random = new Random();

    @Scheduled(fixedRate = 2000)
    public void produceIntStream() {
        numberPublisher.produceRandomNumber(random.nextInt(10));
    }

}
