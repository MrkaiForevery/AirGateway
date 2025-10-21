package com.airfree.flow.config;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class AirPipelineConfig {

    private List<AirOperatorConfig> operators = new ArrayList<>();
}
