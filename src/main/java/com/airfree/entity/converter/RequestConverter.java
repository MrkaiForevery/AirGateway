package com.airfree.entity.converter;

import com.airfree.entity.request.AirGatewayInternalRequest;
import org.springframework.web.reactive.function.server.ServerRequest;

public final class RequestConverter {

    public static AirGatewayInternalRequest converterApiInternalRequest(ServerRequest serverRequest){
        //todo 这里需要实现把这个ServerRequest转换成AirGatewayInternalRequest
        return new AirGatewayInternalRequest();
    }

}
