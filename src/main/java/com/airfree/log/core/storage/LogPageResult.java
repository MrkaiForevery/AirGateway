package com.airfree.log.core.storage;

import java.util.List;

public class LogPageResult<T> {
    private long total;
    private int page;
    private int size;
    private List<T> data;
}
