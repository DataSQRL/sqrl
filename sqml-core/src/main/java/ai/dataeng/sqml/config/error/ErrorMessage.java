package ai.dataeng.sqml.config.error;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.base.Strings;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

public interface ErrorMessage {

    String getMessage();

    Severity getSeverity();

    ErrorLocation getLocation();

    default boolean isFatal() {
        return getSeverity() == Severity.FATAL;
    }

    default boolean isWarning() {
        return getSeverity() == Severity.WARN;
    }

    default boolean isNotice() { return getSeverity() == Severity.NOTICE; }

    default String toStringNoSeverity() {
        String loc = getLocation().toString();
        if (!Strings.isNullOrEmpty(loc)) loc += ": ";
        return loc + getMessage();
    }

    public enum Severity {
        NOTICE, WARN, FATAL;
    }

    @Value
    class Implementation implements ErrorMessage {

        private final String message;
        private final ErrorLocation location;
        private final Severity severity;

        @Override
        public String toString() {
            return "[" + severity + "] " + toStringNoSeverity();
        }

    }

}
