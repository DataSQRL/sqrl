package ai.dataeng.sqml.schema2.basic;

import com.google.common.base.Preconditions;
import lombok.NonNull;
import lombok.Value;

@Value
public class ConversionResult<Result extends Object,E extends ConversionError> {

    private final Result result;
    private final E error;

    private ConversionResult(@NonNull Result result, E error) {
        this.result = result;
        this.error = error;
    }

    private ConversionResult(@NonNull E error) {
        this.result = null;
        this.error = error;
    }

    public boolean hasError() {
        return error!=null;
    }

    public boolean isFatal() {
        return hasError() && error.isFatal();
    }

    public boolean hasWarning() {
        return hasError() && error.isWarning();
    }

    public boolean hasResult() {
        return result!=null;
    }

    public static<R, E extends ConversionError> ConversionResult<R,E> fatal(E error) {
        Preconditions.checkArgument(error.isFatal());
        return new ConversionResult<>(error);
    }

    public static<R> ConversionResult<R, ConversionError> fatal(String format, Object... args) {
        return new ConversionResult<>(new SimpleConversionError(ConversionError.Severity.FATAL, format,args));
    }

    public static<R, E extends ConversionError> ConversionResult<R,E> of(R result) {
        return new ConversionResult<>(result, null);
    }

    public static<R, E extends ConversionError> ConversionResult<R,E> of(R result, E warning) {
        Preconditions.checkArgument(warning.isWarning());
        return new ConversionResult<>(result, warning);
    }


}
