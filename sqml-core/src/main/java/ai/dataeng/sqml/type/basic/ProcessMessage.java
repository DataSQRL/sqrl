package ai.dataeng.sqml.type.basic;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

public interface ProcessMessage {

    String getMessage();

    Severity getSeverity();

    default boolean isFatal() {
        return getSeverity() == Severity.FATAL;
    }

    default boolean isWarning() {
        return getSeverity() == Severity.WARN;
    }

    default boolean isNotice() { return getSeverity() == Severity.NOTICE; }

    public enum Severity {
        NOTICE, WARN, FATAL;
    }

    @Slf4j
    @ToString
    public static class ProcessBundle<E extends ProcessMessage> implements Iterable<E> {

        private List<E> errors;

        public void add(E error) {
            if (errors==null) errors = new ArrayList<>();
            errors.add(error);
        }

        public<T extends E> void addAll(ProcessBundle<T> errors) {
            for (T error : errors) add(error);
        }

        public boolean hasErrors() {
            return !(errors==null || errors.isEmpty());
        }

        public boolean isFatal() {
            return errors!=null && errors.stream().anyMatch(e -> e.isFatal());
        }

        @Override
        public Iterator<E> iterator() {
            if (errors==null) return Collections.emptyIterator();
            return errors.iterator();
        }

        public void merge(ProcessBundle<E> other) {
            if (other==null) return;
            for (E err : other) add(err);
        }

        public static void logMessages(ProcessBundle<ProcessMessage> messages) {
            for (ProcessMessage message : messages) {
                if (message.isNotice()) {
                    log.info(message.toString());
                } else if (message.isWarning()) {
                    log.warn(message.toString());
                } else if (message.isFatal()) {
                    log.error(messages.toString());
                }
            }
        }
    }

}
