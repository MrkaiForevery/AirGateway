package com.airfree.listener.configChangeListener.baseNacos.nacosConfigChangeListener;

import com.airfree.mq.cofig.rocketMq.ConsumerConfig;
import com.airfree.mq.cofig.rocketMq.ProducerConfig;
import com.airfree.mq.core.AirRocketMqBatchConsumerFactory;
import com.airfree.mq.core.AirRocketMqBatchProducerFactory;
import com.alibaba.nacos.api.config.ConfigChangeEvent;
import com.alibaba.nacos.api.config.ConfigChangeItem;
import com.alibaba.nacos.api.config.PropertyChangeType;
import com.alibaba.nacos.client.config.listener.impl.AbstractConfigChangeListener;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.*;

@Slf4j
@Component
public class RocketMqConfigChangeListener extends AbstractConfigChangeListener {

    @Resource
    private AirRocketMqBatchProducerFactory producerFactory;

    @Resource
    private AirRocketMqBatchConsumerFactory consumerFactory;

    @Override
    public void receiveConfigChange(ConfigChangeEvent configChangeEvent) {
        log.info("监听到nacos-rocketMq刷新事件，同步更新rockeMq-producer-consumer实例....");
        Collection<ConfigChangeItem> changeItems = configChangeEvent.getChangeItems();
        //过滤删除、修改、新增的配置不对的的操作
        if (!validateConfigChangesIsAllow(changeItems)) {
            return; // 验证失败，结束方法
        }
        //获取producer/consumer相关的字段
        Map<String, String> producersFiledMap = filterFiledByType(changeItems, "producers");
        Map<String, String> consumersFiledMap = filterFiledByType(changeItems, "consumers");
        //第一步判断要加多少个
        Map<String, Map<String, String>> producersFiledMapLevel2 = buildAddConfigStep1(producersFiledMap);
        Map<String, Map<String, String>> consumersFiledMapLevel2 = buildAddConfigStep1(consumersFiledMap);
        //构建producerConfigs
        Map<String, ProducerConfig> producerConfigs = buildAllAddProducerConfig(producersFiledMapLevel2);
        //构建consumerConfigs
        Map<String, ConsumerConfig> consumerConfigs = buildAllAddConsumerConfig(consumersFiledMapLevel2);
        //重新构建producers
        producerFactory.buildProducerRocketMQTemplate(producerConfigs);
        log.info("构建新怎加producers成功！！！");
        //重新构建consumers
        consumerConfigs.entrySet().forEach(config -> {
            try {
                consumerFactory.buildOneTypeConsumer(config.getValue());
                log.info("构建新增加的consumers成功！！！");
            } catch (ClassNotFoundException e) {
                log.info("构建新增加的consumers失败！！！");
                e.printStackTrace();
            }
        });
    }

    private boolean validateConfigChangesIsAllow(Collection<ConfigChangeItem> changeItems) {
        for (ConfigChangeItem item : changeItems) {
            PropertyChangeType type = item.getType();
            if (PropertyChangeType.DELETED.equals(type)) {
                log.error("rocketMq的配置不允许进行删除操作，只能在原有基础上进行新增操作，或者重启整个服务重新构建已达到效果！！！");
                return false;
            }
            if (PropertyChangeType.MODIFIED.equals(type)) {
                log.error("rocketMq的配置不允许进行修改操作，只能在原有基础上进行新增操作，或者重启整个服务重新构建已达到效果！！！");
                return false;
            }
            if (!verifyAddKey(item.getKey(), item.getNewValue())) {
                log.error("rocketMq的配置不符合规范，请检查确认配置！！！");
                return false;
            }
        }
        return true;
    }

    private Map<String, ConsumerConfig> buildAllAddConsumerConfig(Map<String, Map<String, String>> consumersFiledMapLevel2) {
        Map<String, ConsumerConfig> consumerConfigs = new HashMap<>();
        consumersFiledMapLevel2.entrySet().forEach(consumersEntry -> {
            ConsumerConfig consumerConfig = new ConsumerConfig();
            HashMap<String, String> tags = new HashMap<>();
            consumerConfig.setTags(tags);
            //设置字段
            consumersEntry.getValue().entrySet().forEach(filed -> {
                if (filed.getKey().contains(".group")) {
                    consumerConfig.setGroup(filed.getValue());
                }
                if (filed.getKey().contains(".topic")) {
                    consumerConfig.setTopic(filed.getValue());
                }
                if (filed.getKey().contains(".listenerClass")) {
                    consumerConfig.setListenerClass(filed.getValue());
                }
                if (filed.getKey().contains(".instanceNums")) {
                    consumerConfig.setInstanceNums(filed.getValue());
                }
                if (filed.getKey().contains(".tags")) {
                    List<String> keyParts = Arrays.asList(filed.getKey().split("\\."));
                    consumerConfig.getTags().put(keyParts.get(keyParts.size() - 1), filed.getValue());
                }

            });
            consumerConfigs.put(consumersEntry.getKey(), consumerConfig);
        });
        return consumerConfigs;
    }

    private Map<String, ProducerConfig> buildAllAddProducerConfig(Map<String, Map<String, String>> producersFiledMapLevel2) {
        Map<String, ProducerConfig> producerConfigs = new HashMap<>();
        producersFiledMapLevel2.entrySet().forEach(producerFilesMapEntry -> {
            ProducerConfig producerConfig = new ProducerConfig();
            HashMap<String, String> topics = new HashMap<>();
            producerConfig.setTopics(topics);
            //设置字段
            producerFilesMapEntry.getValue().entrySet().forEach(filed -> {
                if (filed.getKey().contains(".group")) {
                    producerConfig.setGroup(filed.getValue());
                }
                if (filed.getKey().contains(".topics")) {
                    List<String> keyParts = Arrays.asList(filed.getKey().split("\\."));
                    producerConfig.getTopics().put(keyParts.get(keyParts.size() - 1), filed.getValue());
                }
            });
            producerConfigs.put(producerFilesMapEntry.getKey(), producerConfig);
        });
        return producerConfigs;
    }


    private Map<String, Map<String, String>> buildAddConfigStep1(Map<String, String> producersFiledMap) {
        //先判断有多少个新增的producer配置
        Map<String, Map<String, String>> level2Map = new HashMap<>();
        producersFiledMap.entrySet().forEach(filed -> {
            List<String> keyParts = Arrays.asList(filed.getKey().split("\\."));
            String newKey = keyParts.get(2);
            if (level2Map.get(newKey) != null) {
                level2Map.get(newKey).put(filed.getKey(), filed.getValue());
            } else {
                Map<String, String> level3Map = new HashMap<>();
                level3Map.put(filed.getKey(), filed.getValue());
                level2Map.put(newKey, level3Map);
            }
        });
        return level2Map;
    }

    private Map<String, String> filterFiledByType(Collection<ConfigChangeItem> changeItems, String type) {
        Map<String, String> filedMap = new HashMap<>();
        changeItems.forEach(e -> {
            String filedKey = e.getKey();
            List<String> keyParts = Arrays.asList(filedKey.split("\\."));
            if (type.equals(keyParts.get(1))) {
                filedMap.put(filedKey, e.getNewValue());
            }
        });
        return filedMap;
    }

    private boolean verifyAddKey(String key, String value) {
        if (value == null || "".equals(value)) {
            return false;
        }
        List<String> keyParts = Arrays.asList(key.split("\\."));
        if (keyParts.size() < 4) {
            return false;
        }
        Map<Integer, String> keyMap = new HashMap<>();
        for (int i = 0; i < keyParts.size(); i++) {
            keyMap.put(i, keyParts.get(i));
        }

        if (!"rocketmq".equals(keyMap.get(0))) {
            return false;
        }
        if ("producers".equals(keyMap.get(1)) && ("group".equals(keyMap.get(3)) || "topics".equals(keyMap.get(3)))) {
            return true;
        }

        if ("consumers".equals(keyMap.get(1)) && ("group".equals(keyMap.get(3)) || "topic".equals(keyMap.get(3)) ||
                "tags".equals(keyMap.get(3))) || "listenerClass".equals(keyMap.get(3)) || "instanceNums".equals(keyMap.get(3))) {
            return true;
        }
        return false;
    }


}
