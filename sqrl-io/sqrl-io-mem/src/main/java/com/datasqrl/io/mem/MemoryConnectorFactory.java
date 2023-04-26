package com.datasqrl.io.mem;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.io.DataSystemConnectorSettings;
import com.datasqrl.io.DataSystemConnectorFactory;
import lombok.Getter;
import lombok.NonNull;

@Getter
public abstract class MemoryConnectorFactory implements DataSystemConnectorFactory {

    public static final String SYSTEM_NAME_PREFIX = "in-mem-";

    private final String systemName;

    public MemoryConnectorFactory(String name) {
        systemName = SYSTEM_NAME_PREFIX + name;
    }

    @Override
    public DataSystemConnectorSettings getSettings(@NonNull SqrlConfig connectorConfig) {
        return DataSystemConnectorSettings.builder().hasSourceTimestamp(false).build();
    }

}
