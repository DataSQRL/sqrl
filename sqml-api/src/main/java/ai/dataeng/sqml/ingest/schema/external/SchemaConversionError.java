package ai.dataeng.sqml.ingest.schema.external;

import ai.dataeng.sqml.schema2.basic.ConversionError;
import ai.dataeng.sqml.schema2.basic.SimpleConversionError;
import ai.dataeng.sqml.schema2.name.NamePath;
import lombok.Value;

@Value
public class SchemaConversionError extends SimpleConversionError {

    protected final NamePath location;

    public SchemaConversionError(Severity severity, NamePath location, String msg, Object value) {
        super(severity, msg, value);
        this.location = location;
    }

    public SchemaConversionError(NamePath location, ConversionError error) {
        super(error);
        this.location = location;
    }

    public SchemaConversionError(Severity severity, NamePath location, String msg, Object... args) {
        super(severity, msg, args);
        this.location = location;
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
