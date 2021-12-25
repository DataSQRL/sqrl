package ai.dataeng.sqml.type.basic;

public class SimpleConversionError implements ProcessMessage {

    protected final String msg;
    protected final Object value;
    protected final Severity severity;

    protected SimpleConversionError(ProcessMessage o) {
        if (o instanceof SimpleConversionError) {
            SimpleConversionError other = (SimpleConversionError)o;
            this.msg = other.msg;
            this.value = other.value;
            this.severity = other.severity;
        } else {
            this.msg = o.getMessage();
            this.severity = o.getSeverity();
            this.value = null;
        }
    }

    public SimpleConversionError(Severity severity, String format, Object... args) {
        this(severity, String.format(format, args),
                (args != null && args.length > 0) ? args[0] : null);
    }

    public SimpleConversionError(Severity severity, String msg, Object value) {
        this.severity = severity;
        this.msg = msg;
        this.value = value;
    }

    @Override
    public Severity getSeverity() {
        return severity;
    }

    @Override
    public String getMessage() {
        return msg;
    }

    public Object getValue() {
        return value;
    }

    @Override
    public String toString() {
        return severity.name() + "[" + msg + "]"; //value=" + Objects.toString(value);
    }


}
