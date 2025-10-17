package com.airfree.rpc.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum AirRPCTypeEnum {

    RSOCKET("RSocket"),
    GRPC("gRPC"),
    HTTP("http");

    private String rpcName;
}
