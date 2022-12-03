package com.datasqrl.io.sources;

public interface DataSystemConnector {

    boolean hasSourceTimestamp();

    default boolean requiresFormat(ExternalDataType type) {
        return true;
    }

}
