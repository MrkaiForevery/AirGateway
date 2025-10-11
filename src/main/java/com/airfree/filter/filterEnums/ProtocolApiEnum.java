package com.airfree.filter.filterEnums;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum ProtocolApiEnum {

    PROTOCOL_API_SOAP("internet_protocol_type","api_soap"),
    PROTOCOL_API_REST("internet_protocol_type","api_rest"),
    PROTOCOL_NOT_SUPPORT_TYPE("internet_protocol_type","unsupported_api_type");

    private String typeKey;
    private String typeValue;
}
