package com.datasqrl.io.sources;

public enum ExternalDataType {
    SOURCE, SINK, SOURCE_AND_SINK;

    public boolean isSource() {
        return this == SOURCE || this == SOURCE_AND_SINK;
    }

    public boolean isSink() {
        return this == SINK || this == SOURCE_AND_SINK;
    }
}
