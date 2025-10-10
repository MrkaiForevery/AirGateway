package com.airfree.filter.customizeFilter;

import com.airfree.filter.AbstractAirGatewayFilter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.http.server.reactive.ServerHttpRequestDecorator;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.server.ServerResponse;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.Map;

@Slf4j
@Component
public class ApiInternalRequestConvertFilter  extends AbstractAirGatewayFilter {

    private static final String INTERNET_PROTOCOL_TYPE = "internet_protocol_type";
    private static final String API_SOAP = "api_soap";
    private static final String API_REST = "api_rest";
    private static final String NOT_SUPPORT = "unsupported_api_type";

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        setNSupportAttributes(exchange);
        String apiType = (String) exchange.getAttributes().get(INTERNET_PROTOCOL_TYPE);
        if (NOT_SUPPORT.equals(exchange.getAttributes().get(INTERNET_PROTOCOL_TYPE))) {
            //then()会导致直接返回，不会执行下面的filter
            log.info("拒绝请求，终止过滤器链");
            return ServerResponse.status(HttpStatus.UNSUPPORTED_MEDIA_TYPE)
                    .bodyValue("unsupported api request")
                    .then();
        }
        return DataBufferUtils.join(exchange.getRequest().getBody())
                .flatMap(dataBuffer -> {
                            try {
                                // 读取内容
                                byte[] bytes = new byte[dataBuffer.readableByteCount()];
                                dataBuffer.read(bytes);
                                DataBufferUtils.release(dataBuffer);
                                String originalContent = new String(bytes, StandardCharsets.UTF_8);
                                String transformedContent = null;
                                //内容转换,根据不同协议执行不同的转换逻辑
                                if (API_SOAP.equals(apiType)) {
                                    transformedContent = transformSoapContent(originalContent, exchange);
                                }
                                if (API_REST.equals(apiType)) {
                                    transformedContent = transformRestContent(originalContent, exchange);
                                }
                                byte[] transformedBytes = transformedContent.getBytes(StandardCharsets.UTF_8);
                                //重新构建request
                                ServerHttpRequest mutatedRequest = new ServerHttpRequestDecorator(exchange.getRequest()) {
                                    @Override
                                    public Flux<DataBuffer> getBody() {
                                        return Flux.just(exchange.getResponse().bufferFactory().wrap(transformedBytes));
                                    }

                                    @Override
                                    public org.springframework.http.HttpHeaders getHeaders() {
                                        // 因为我们修改了body，所以需要更新Content-Length
                                        org.springframework.http.HttpHeaders headers = new org.springframework.http.HttpHeaders();
                                        headers.putAll(super.getHeaders());
                                        headers.remove(HttpHeaders.TRANSFER_ENCODING);
                                        headers.setContentLength(transformedBytes.length);
                                        // 如果Content-Type需要修改，也可以在这里设置
                                        // headers.setContentType(MediaType.TEXT_XML);
                                        return headers;
                                    }
                                };
                                //用构建好的request进行下一个filter的执行
                                return chain.filter(exchange.mutate().request(mutatedRequest).build());
                            } catch (Exception e) {
                                return Mono.error(e);
                            }
                        }
                );
    }

    private void setNSupportAttributes(ServerWebExchange exchange) {
        MediaType contentType = exchange.getRequest().getHeaders().getContentType();
        Map<String, Object> exchangeAttributes = exchange.getAttributes();
        if (contentType != null && (contentType.includes(MediaType.TEXT_XML) || contentType.includes(MediaType.APPLICATION_XML))) {
            exchangeAttributes.put(INTERNET_PROTOCOL_TYPE, API_SOAP);
        }
        ;
        if (contentType != null && contentType.includes(MediaType.APPLICATION_JSON)) {
            exchangeAttributes.put(INTERNET_PROTOCOL_TYPE, API_REST);
        }
        exchangeAttributes.put(INTERNET_PROTOCOL_TYPE, NOT_SUPPORT);
    }

    private String transformSoapContent(String originalContent, ServerWebExchange exchange) {
        // todo SOAP 请求转换逻辑
        // 例如：添加认证头、修改端点等
        return originalContent.replace(
                "<soap:Envelope",
                "<soap:Envelope xmlns:custom=\"http://custom.namespace\""
        );
    }

    private String transformRestContent(String originalContent, ServerWebExchange exchange) {
        // todo  请求转换逻辑
        return originalContent.replace(
                "<soap:Envelope",
                "<soap:Envelope xmlns:custom=\"http://custom.namespace\""
        );
    }

    @Override
    public void enable() {
        enabled.set(true);
    }

    @Override
    public void disable() {
        enabled.set(false);
    }
}
