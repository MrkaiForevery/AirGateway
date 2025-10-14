package com.airfree.mq.mqEnums;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum MqTypeEnum {

    ROCKET_MQ("rocketMq","v-4.2.3"),
    KAFKA("kafka","v-4.5.6");

    private String mqName;
    private String mqVersion;
}
