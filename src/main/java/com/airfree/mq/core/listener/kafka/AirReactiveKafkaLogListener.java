package com.airfree.mq.core.listener.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;

@Slf4j
@Component
public class AirReactiveKafkaLogListener implements AirReactiveKafkaListener<String, String> {

    @Override
    public Mono<Void> onMessage(ReceiverRecord<String, String> record) {
        log.info("接收到kafka的消息，正在处理中....");
        // 处理业务逻辑
        return processMessage(record.value())
                .doOnSuccess(v -> {
                    log.debug("业务处理成功完成 - 主题: {}, 键: {}", record.topic(), record.key());
                })
                .doOnError(error -> {
                    log.error("业务处理失败 - 主题: {}, 键: {}, 错误: {}",
                            record.topic(), record.key(), error.getMessage());
                    // 这里不要处理错误，让错误传播到上层进行统一处理
                });
    }

    /**
     * 具体的消息处理逻辑
     */
    private Mono<Void> processMessage(String message) {
        return Mono.fromRunnable(() -> {
            try {
                // 这里写你的具体业务逻辑
                log.info("开始处理消息内容: {}", message);

                // 示例业务逻辑：
                // 1. 解析消息
                // 2. 验证数据
                // 3. 调用其他服务
                // 4. 保存到数据库

                // 模拟业务处理
                if (message == null || message.trim().isEmpty()) {
                    throw new IllegalArgumentException("消息内容为空");
                }

                if (message.contains("模拟错误")) {
                    throw new RuntimeException("模拟业务处理错误");
                }

                // 正常处理逻辑
                log.info("消息处理完成: {}", message);

            } catch (Exception e) {
                log.error("处理消息时发生异常: {}", e.getMessage(), e);
                throw new RuntimeException("业务处理失败", e);
            }
        });
    }

    @Override
    public String getSupportedTopic() {
        return "air_log_topic";
    }

    @Override
    public String getListenerName() {
        return "consumer-log";
    }
}
