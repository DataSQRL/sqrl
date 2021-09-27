package ai.dataeng.sqml.ingest.schema;

import ai.dataeng.sqml.ingest.LocationConversionError;
import ai.dataeng.sqml.schema2.basic.ConversionError;
import ai.dataeng.sqml.schema2.name.NamePath;
import lombok.NonNull;

public class SchemaConversionError extends LocationConversionError<NamePath> {

    public SchemaConversionError(Severity severity, @NonNull NamePath location, String msg, Object value) {
        super(severity, location, msg, value);
    }

    public SchemaConversionError(@NonNull NamePath location, ConversionError error) {
        super(location, error);
    }

    public SchemaConversionError(Severity severity, @NonNull NamePath location, String msg, Object... args) {
        super(severity, location, msg, args);
    }

    public static SchemaConversionError fatal(NamePath location, String msg, Object... args) {
        return new SchemaConversionError(Severity.FATAL, location, msg, args);
    }

    public static SchemaConversionError warn(NamePath location, String msg, Object... args) {
        return new SchemaConversionError(Severity.WARN, location, msg, args);
    }

    public static SchemaConversionError convert(NamePath location, ConversionError error) {
        return new SchemaConversionError(location,error);
    }

}
