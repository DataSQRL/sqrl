package ai.dataeng.sqml.config;

import ai.dataeng.sqml.execution.flink.ingest.stats.DocumentPath;
import ai.dataeng.sqml.execution.flink.ingest.stats.StatsIngestError;
import ai.dataeng.sqml.type.basic.LocationConversionError;
import ai.dataeng.sqml.type.basic.ProcessMessage;
import lombok.NonNull;
import lombok.Value;

public class ConfigurationError extends LocationConversionError<ConfigurationError.Location> {

    public ConfigurationError(Severity severity, @NonNull ConfigurationError.Location location, String msg, Object... args) {
        super(severity, location, msg, args);
    }

    public static ConfigurationError fatal(LocationType type, String name, String msg, Object... args) {
        return new ConfigurationError(Severity.FATAL, new Location(type,name), msg, args);
    }

    public static ConfigurationError warn(LocationType type, String name, String msg, Object... args) {
        return new ConfigurationError(Severity.WARN, new Location(type,name), msg, args);
    }

    @Value
    public static class Location {

        private final @NonNull LocationType type;
        private final String name;

    }

    public enum LocationType {

        GLOBAL, SOURCE, SINK, ENGINE;

    }

}
