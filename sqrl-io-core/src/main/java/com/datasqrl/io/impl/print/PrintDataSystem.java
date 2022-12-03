package com.datasqrl.io.impl.print;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.*;
import com.datasqrl.io.formats.FileFormat;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.name.Name;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

import java.util.Optional;

@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
@Getter
public abstract class PrintDataSystem {

    public static final String SYSTEM_TYPE = "print";

    String prefix;

    public String getSystemType() {
        return SYSTEM_TYPE;
    }

    private static final PrintDataSystem.Discovery DEFAULT_DISCOVERY = new Discovery();
    public static final DataSystemConfig DEFAULT_DISCOVERY_CONFIG = DataSystemConfig.builder()
            .name(SYSTEM_TYPE)
            .datadiscovery(DEFAULT_DISCOVERY)
            .type(ExternalDataType.SINK)
            .format(FileFormat.JSON.getImplementation().getDefaultConfiguration())
            .build();

    public static Optional<TableConfig> discoverSink(@NonNull Name sinkName, @NonNull ErrorCollector errors) {
        return DEFAULT_DISCOVERY.discoverSink(sinkName, DEFAULT_DISCOVERY_CONFIG,errors);
    }

    @SuperBuilder
    @NoArgsConstructor
    public static class Connector extends PrintDataSystem implements DataSystemConnectorConfig, DataSystemConnector {

        public Connector(String prefix) {
            super(prefix);
        }

        @Override
        public DataSystemConnector initialize(@NonNull ErrorCollector errors) {
            return this;
        }

        @Override
        @JsonIgnore
        public boolean hasSourceTimestamp() {
            return false;
        }
    }

    @SuperBuilder
    @NoArgsConstructor
    public static class Discovery extends PrintDataSystem implements DataSystemDiscoveryConfig, DataSystemDiscovery {

        @Override
        @JsonIgnore
        public @NonNull Optional<String> getDefaultName() {
            return Optional.of(SYSTEM_TYPE);
        }

        @Override
        public boolean requiresFormat(ExternalDataType type) {
            return true;
        }

        @Override
        public Optional<TableConfig> discoverSink(@NonNull Name sinkName, @NonNull DataSystemConfig config, @NonNull ErrorCollector errors) {
            TableConfig.TableConfigBuilder tblBuilder = TableConfig.copy(config);
            tblBuilder.type(ExternalDataType.SINK);
            tblBuilder.identifier(sinkName.getCanonical());
            tblBuilder.name(sinkName.getDisplay());
            tblBuilder.connector(new Connector(prefix));
            return Optional.of(tblBuilder.build());
        }

        @Override
        public DataSystemDiscovery initialize(@NonNull ErrorCollector errors) {
            return this;
        }
    }

}
