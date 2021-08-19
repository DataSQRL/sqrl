package ai.dataeng.sqml.ingest;

import ai.dataeng.sqml.source.SourceRecord;
import com.google.common.base.Preconditions;
import lombok.NonNull;
import lombok.Value;

import java.io.Serializable;
import java.util.Objects;

@Value
public class SchemaAdjustment<D> {

    private static final SchemaAdjustment NONE = new SchemaAdjustment(null, null);

    private final D data;
    private final ErrorMessage error;

    public boolean isError() {
        return error != null;
    }

    public boolean transformedData() {
        return data != null;
    }

    public<T> SchemaAdjustment<T> castError() {
        Preconditions.checkArgument(isError());
        return (SchemaAdjustment<T>) this;
    }

    public static<D> SchemaAdjustment<D> error(NamePath fieldName, Object fieldValue, String message) {
        return new SchemaAdjustment<>(null,new ErrorMessage(fieldName,fieldValue,message));
    }

    public static<D> SchemaAdjustment<D> data(D data) {
        return new SchemaAdjustment<>(data, null);
    }

    public static <D> SchemaAdjustment<D> none() {
        return NONE;
    }


    @Value
    public static class ErrorMessage implements Serializable {

        private final @NonNull NamePath fieldName;
        private final Object fieldValue;
        private final @NonNull String message;

        @Override
        public String toString() {
            return "Error processing [" + fieldName.toString() + "]: " + message + " -- Processed value: " + Objects.toString(fieldValue);
        }

    }

}
