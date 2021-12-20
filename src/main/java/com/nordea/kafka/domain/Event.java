package com.nordea.kafka.domain;

import lombok.Data;

/**
 * @author Radek
 * @since 2021-12-20
 */
@Data
public class Event {

    private Integer number;

    private String name = "EventName";

    private String type;

    private boolean isValid = true;


}
