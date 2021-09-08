package ai.dataeng.sqml.schema2.basic;

public interface ConversionError {

    String getMessage();

    Severity getSeverity();

    default boolean isFatal() {
        return getSeverity() == Severity.FATAL;
    }

    default boolean isWarning() {
        return getSeverity() == Severity.WARN;
    }

    public enum Severity {
        WARN, FATAL;
    }

}
