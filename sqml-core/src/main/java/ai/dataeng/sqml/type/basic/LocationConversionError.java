package ai.dataeng.sqml.type.basic;

import ai.dataeng.sqml.type.basic.ProcessMessage;
import ai.dataeng.sqml.type.basic.SimpleConversionError;
import lombok.NonNull;

public class LocationConversionError<L> extends SimpleConversionError {

    protected final L location;

    public LocationConversionError(Severity severity, @NonNull L location, String msg, Object value) {
        super(severity, msg, value);
        this.location = location;
    }

    public LocationConversionError(@NonNull L location, ProcessMessage error) {
        super(error);
        this.location = location;
    }

    public LocationConversionError(Severity severity, @NonNull L location, String msg, Object... args) {
        super(severity, msg, args);
        this.location = location;
    }

    public L getLocation() {
        return location;
    }

    @Override
    public String toString() {
        return super.toString() + ";location=" + location.toString();
    }

}
