package ai.dataeng.sqml.config;

import ai.dataeng.sqml.type.basic.LocationConversionError;
import lombok.NonNull;
import lombok.ToString;
import lombok.Value;

public class ConfigurationError extends LocationConversionError<ConfigurationError.Location> {

    public ConfigurationError(Severity severity, @NonNull ConfigurationError.Location location, String msg, Object... args) {
        super(severity, location, msg, args);
    }

    public static ConfigurationError fatal(LocationType type, String location, String msg, Object... args) {
        return new ConfigurationError(Severity.FATAL, new Location(type,location), msg, args);
    }

    public static ConfigurationError warn(LocationType type, String location, String msg, Object... args) {
        return new ConfigurationError(Severity.WARN, new Location(type,location), msg, args);
    }

    @Value
    public static class Location {

        private final @NonNull LocationType type;
        private final String location;

        @Override
        public String toString() {
            return location;
        }

    }

    public enum LocationType {

        GLOBAL, SOURCE, SINK, ENGINE;

    }

}
