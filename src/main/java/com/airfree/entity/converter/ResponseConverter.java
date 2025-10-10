package com.airfree.entity.converter;

import com.airfree.entity.response.AirGatewayInternalResponse;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.web.ErrorResponse;
import org.springframework.web.ErrorResponseException;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Mono;

public final class ResponseConverter {

    //todo 这里只是一个实现的例子，具体业务逻辑还没编写
    public static Mono<ServerResponse> converterApiInternalResponse(Mono<AirGatewayInternalResponse> internalResponseMono) {
        return internalResponseMono
                .flatMap(internalResponse -> {
                    try {
                        // 验证状态码
                        int statusCode = internalResponse.getStatus() > 0 ? internalResponse.getStatus() : 200;
                        if (statusCode < 100 || statusCode >= 600) {
                            statusCode = 500; // 无效状态码时使用默认值
                        }

                        ServerResponse.BodyBuilder responseBuilder = ServerResponse.status(statusCode);

                        // 构建响应体
                        Object responseBody = internalResponse.getBody();
                        if (responseBody != null) {
                            return responseBuilder.body(BodyInserters.fromValue(responseBody));
                        } else {
                            return responseBuilder.build();
                        }
                    } catch (Exception e) {
                        return Mono.error(new RuntimeException("Failed to build server response", e));
                    }
                })
                .onErrorResume(throwable -> buildErrorResponse(throwable));
    }

    private static Mono<ServerResponse> buildErrorResponse(Throwable throwable) {
        ErrorResponse errorResponse = new ErrorResponseException(HttpStatusCode.valueOf(500),throwable);

        return ServerResponse.status(500)
                .contentType(MediaType.APPLICATION_JSON)
                .body(BodyInserters.fromValue(errorResponse));
    }
}
