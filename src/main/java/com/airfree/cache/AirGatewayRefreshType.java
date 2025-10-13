package com.airfree.cache;

public enum AirGatewayRefreshType {

    FULL,      // 全量刷新
    PARTIAL,   // 部分刷新
    KEY_BASED, // 基于key刷新
    MANUAL,    // 手动刷新
    SCHEDULED  // 定时刷新
}
