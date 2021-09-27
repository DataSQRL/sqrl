package ai.dataeng.sqml.schema2.basic;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

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

    public static class Bundle<E extends ConversionError> implements Iterable<E> {

        private List<E> errors;

        public void add(E error) {
            if (errors==null) errors = new ArrayList<>();
            errors.add(error);
        }

        public boolean hasErrors() {
            return errors==null || !errors.isEmpty();
        }

        public boolean isFatal() {
            return errors!=null && errors.stream().anyMatch(e -> e.isFatal());
        }

        @Override
        public Iterator<E> iterator() {
            if (errors==null) return Collections.emptyIterator();
            return errors.iterator();
        }
    }

}
