package com.airfree.flow.config;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashMap;
import java.util.Map;

@Setter
@Getter
@NoArgsConstructor
@ConfigurationProperties(prefix = "flowControl")
public class AirFlowControlConfigProperties {

    private Map<String, AirPipelineConfig> pipelines = new HashMap<>();

}
