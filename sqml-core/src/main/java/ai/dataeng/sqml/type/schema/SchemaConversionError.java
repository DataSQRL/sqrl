package ai.dataeng.sqml.type.schema;

import ai.dataeng.sqml.type.basic.LocationConversionError;
import ai.dataeng.sqml.type.basic.ProcessMessage;
import ai.dataeng.sqml.tree.name.NamePath;
import lombok.NonNull;

public class SchemaConversionError extends LocationConversionError<NamePath> {

    public SchemaConversionError(Severity severity, @NonNull NamePath location, String msg, Object value) {
        super(severity, location, msg, value);
    }

    public SchemaConversionError(@NonNull NamePath location, ProcessMessage error) {
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

    public static SchemaConversionError notice(NamePath location, String msg, Object... args) {
        return new SchemaConversionError(Severity.NOTICE, location, msg, args);
    }

    public static SchemaConversionError convert(NamePath location, ProcessMessage error) {
        return new SchemaConversionError(location,error);
    }

}
