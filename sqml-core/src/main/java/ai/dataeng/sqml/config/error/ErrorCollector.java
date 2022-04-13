package ai.dataeng.sqml.config.error;

import ai.dataeng.sqml.tree.NodeLocation;
import ai.dataeng.sqml.tree.name.Name;
import io.vertx.json.schema.ValidationException;
import io.vertx.json.schema.common.ValidationExceptionImpl;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class ErrorCollector implements Iterable<ErrorMessage> {

    private final ErrorLocation baseLocation;
    private final List<ErrorMessage> errors;

    private ErrorCollector(@NonNull ErrorLocation location, @NonNull List<ErrorMessage> errors) {
        this.baseLocation = location;
        this.errors = errors;
    }

    private ErrorCollector(@NonNull ErrorLocation location) {
        this(location,new ArrayList<>(5));
    }

    public static ErrorCollector root() {
        return new ErrorCollector(ErrorPrefix.ROOT);
    }

    public static ErrorCollector fromPrefix(@NonNull ErrorPrefix prefix) {
        return new ErrorCollector(prefix);
    }

    public ErrorCollector resolve(String location) {
        return new ErrorCollector(baseLocation.resolve(location),errors);
    }

    public ErrorCollector resolve(Name location) {
        return new ErrorCollector(baseLocation.resolve(location),errors);
    }

    private void addInternal(@NonNull ErrorMessage error) {
        errors.add(error);
    }

    public void add(@NonNull ErrorMessage err) {
        ErrorLocation errLoc = err.getLocation();
        if (!errLoc.hasPrefix()) {
            //Adjust relative location
            ErrorLocation newloc = baseLocation.append(errLoc);
            err = new ErrorMessage.Implementation(err.getMessage(),newloc,err.getSeverity());
        }
        addInternal(err);
    }

    public void addAll(ErrorCollector other) {
        if (other == null) return;
        for (ErrorMessage err : other) {
            add(err);
        }
    }


    public boolean hasErrors() {
        return !errors.isEmpty();
    }

    public boolean isFatal() {
        return errors.stream().anyMatch(ErrorMessage::isFatal);
    }

    public boolean isSuccess() {
        return !isFatal();
    }

    public void fatal(String msg, Object... args) {
        addInternal(new ErrorMessage.Implementation(getMessage(msg,args),baseLocation, ErrorMessage.Severity.FATAL));
    }

    public void fatal(int line, int offset, String msg, Object... args) {
        addInternal(new ErrorMessage.Implementation(getMessage(msg,args),baseLocation.atFile(line,offset), ErrorMessage.Severity.FATAL));
    }

    public void fatal(NodeLocation location, String msg, Object... args) {
        fatal(location.getLineNumber(),location.getColumnNumber(),msg,args);
    }

    public void fatal(ValidationException exception) {
        String msg = exception.getMessage() + " [ keyword = " + exception.keyword() + "]";
        //Convert JSON location to error location. The only access we have to the JSONPointer path is through toString() and we need to parse
        ErrorLocation loc = baseLocation;
        if (exception.inputScope()!=null) {
            String[] path = StringUtils.split(exception.inputScope().toString(), "/");
            for (String p : path) {
                if (!p.isBlank()) loc = loc.resolve(p);
            }
        }
        addInternal(new ErrorMessage.Implementation(msg, loc, ErrorMessage.Severity.FATAL));
    }


    public void warn(String msg, Object... args) {
        addInternal(new ErrorMessage.Implementation(getMessage(msg,args),baseLocation, ErrorMessage.Severity.WARN));
    }

    public void warn(int line, int offset, String msg, Object... args) {
        addInternal(new ErrorMessage.Implementation(getMessage(msg,args),baseLocation.atFile(line,offset), ErrorMessage.Severity.WARN));
    }

    public void notice(String msg, Object... args) {
        addInternal(new ErrorMessage.Implementation(getMessage(msg,args),baseLocation, ErrorMessage.Severity.NOTICE));
    }

    public void notice(int line, int offset, String msg, Object... args) {
        addInternal(new ErrorMessage.Implementation(getMessage(msg,args),baseLocation.atFile(line,offset), ErrorMessage.Severity.NOTICE));
    }

    private static String getMessage(String msgTemplate, Object... args) {
        if (args==null || args.length == 0) return msgTemplate;
        return String.format(msgTemplate, args);
    }

    @Override
    public Iterator<ErrorMessage> iterator() {
        return errors.iterator();
    }

    public String combineMessages(ErrorMessage.Severity minSeverity, String prefix, String delimiter) {
        String suffix = "";
        if (errors != null)
            suffix = errors.stream().filter(m -> m.getSeverity().compareTo(minSeverity) >= 0).map(ErrorMessage::toString)
                    .collect(Collectors.joining(delimiter));
        return prefix + suffix;
    }

    @Override
    public String toString() {
        return combineMessages(ErrorMessage.Severity.NOTICE, "", "\n");
    }

    public List<ErrorMessage> getAll() {
        return new ArrayList<>(errors);
    }

    public void log() {
        for (ErrorMessage message : errors) {
            if (message.isNotice()) {
                log.info(message.toStringNoSeverity());
            } else if (message.isWarning()) {
                log.warn(message.toStringNoSeverity());
            } else if (message.isFatal()) {
                log.error(message.toStringNoSeverity());
            } else throw new UnsupportedOperationException("Unexpected severity: " + message.getSeverity());
        }
    }
}
