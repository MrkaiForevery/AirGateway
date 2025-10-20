package com.airfree.flow.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

//todo 这个待搞清楚逻辑，作为扩展用户操作方法需要，先提交一版把
public class AirFlowControlDsl {

    // 入口方法
    public static AirFlowControlDsl define() {
        return new AirFlowControlDsl();
    }

    // 资源定义
    public ResourceDefinition forResource(String resource) {
        return new ResourceDefinition(resource);
    }

    // 资源定义类
    public static class ResourceDefinition {
        private final String resource;
        private final List<FlowControlRule> rules = new ArrayList<>();

        public ResourceDefinition(String resource) {
            this.resource = resource;
        }

        // 限流规则
        public RateLimitDefinition rateLimit() {
            return new RateLimitDefinition(this);
        }

        // 熔断规则
        public CircuitBreakerDefinition circuitBreaker() {
            return new CircuitBreakerDefinition(this);
        }

        // 降级规则
        public DegradeDefinition degrade() {
            return new DegradeDefinition(this);
        }

        // 结束定义，返回父级
        public AirFlowControlDsl end() {
            return AirFlowControlDsl.this;
        }

        public List<FlowControlRule> buildRules() {
            return rules;
        }
    }

    // 限流定义
    public static class RateLimitDefinition {
        private final ResourceDefinition parent;
        private String algorithm = "TOKEN_BUCKET";
        private int capacity = 100;
        private int refillRate = 10;

        public RateLimitDefinition(ResourceDefinition parent) {
            this.parent = parent;
        }

        public RateLimitDefinition tokenBucket() {
            this.algorithm = "TOKEN_BUCKET";
            return this;
        }

        public RateLimitDefinition slidingWindow() {
            this.algorithm = "SLIDING_WINDOW";
            return this;
        }

        public RateLimitDefinition capacity(int capacity) {
            this.capacity = capacity;
            return this;
        }

        public RateLimitDefinition refillRate(int rate) {
            this.refillRate = rate;
            return this;
        }

        public RateLimitDefinition qps(int qps) {
            this.capacity = qps;
            this.refillRate = qps;
            return this;
        }

        // 返回父级继续配置
        public ResourceDefinition end() {
            // 构建规则并添加到父级
            FlowControlRule rule = new FlowControlRule();
            rule.setType("RATE_LIMIT");
            rule.setConfig(buildConfig());
            parent.rules.add(rule);
            return parent;
        }

        private Map<String, Object> buildConfig() {
            Map<String, Object> config = new HashMap<>();
            config.put("algorithm", algorithm);
            config.put("capacity", capacity);
            config.put("refillRate", refillRate);
            return config;
        }
    }

    // 类似的熔断器、降级定义类...
    public static class CircuitBreakerDefinition {
        private final ResourceDefinition parent;
        private double failureThreshold = 0.5;
        private long waitDuration = 30000;

        public CircuitBreakerDefinition(ResourceDefinition parent) {
            this.parent = parent;
        }

        public CircuitBreakerDefinition failureThreshold(double threshold) {
            this.failureThreshold = threshold;
            return this;
        }

        public CircuitBreakerDefinition waitDuration(long duration) {
            this.waitDuration = duration;
            return this;
        }

        public CircuitBreakerDefinition waitSeconds(int seconds) {
            this.waitDuration = seconds * 1000L;
            return this;
        }

        public ResourceDefinition end() {
            FlowControlRule rule = new FlowControlRule();
            rule.setType("CIRCUIT_BREAKER");
            rule.setConfig(buildConfig());
            parent.rules.add(rule);
            return parent;
        }

        private Map<String, Object> buildConfig() {
            Map<String, Object> config = new HashMap<>();
            config.put("failureThreshold", failureThreshold);
            config.put("waitDuration", waitDuration);
            return config;
        }
    }
}
