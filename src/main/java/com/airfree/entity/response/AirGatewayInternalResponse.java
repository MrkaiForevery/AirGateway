package com.airfree.entity.response;

import io.netty.handler.codec.http.HttpHeaders;
import lombok.Data;
import lombok.Getter;

@Data
@Getter
public class AirGatewayInternalResponse {
    private int status;
    private HttpHeaders headers;
    private byte[] body;
    private long processingTime;

    public static AirGatewayInternalResponse of(int status, HttpHeaders headers, byte[] body) {
        AirGatewayInternalResponse response = new AirGatewayInternalResponse();
        response.setStatus(status);
        response.setHeaders(headers);
        response.setBody(body);
        return response;
    }

}
