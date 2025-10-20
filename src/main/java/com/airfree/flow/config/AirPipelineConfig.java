package com.airfree.flow.config;

import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Setter
@Getter
@NoArgsConstructor
public class AirPipelineConfig {
    private List<AirOperatorConfig> operators = new ArrayList<>();
}
