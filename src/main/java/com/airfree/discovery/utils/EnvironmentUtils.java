package com.airfree.discovery.utils;

import lombok.extern.slf4j.Slf4j;
import org.springframework.core.env.Environment;

import java.net.InetAddress;

@Slf4j
public class EnvironmentUtils {

    /**
     * 从环境变量获取端口
     */
    public static int getServerPort(Environment environment) {
        // 优先级：环境变量 > 系统属性 > 默认值
        Integer port = environment.getProperty("GATEWAY_PORT", Integer.class,
                environment.getProperty("gateway.port", Integer.class,
                        environment.getProperty("server.port", Integer.class, 10101)));

        log.debug("获取到网关端口: {}", port);
        return port;
    }

    /**
     * 从环境变量获取主机地址
     */
    public static String getLocalHost(Environment environment) {
        // 优先级：环境变量 > 系统属性 > 网络接口 > 默认值
        String host = environment.getProperty("GATEWAY_HOST",
                environment.getProperty("gateway.host",
                        environment.getProperty("server.address",
                                getLocalHostAddress())));

        log.debug("获取到网关主机地址: {}", host);
        return host;
    }

    /**
     * 获取本地网络地址（备选方案）
     */
    public static String getLocalHostAddress() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (Exception e) {
            log.warn("获取本地主机地址失败，使用回退地址", e);
            return "127.0.0.1";
        }
    }
}
