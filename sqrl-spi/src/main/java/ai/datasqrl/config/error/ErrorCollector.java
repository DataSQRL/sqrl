package ai.datasqrl.config.error;

import ai.datasqrl.parse.tree.name.Name;
import lombok.Getter;
import lombok.NonNull;

import java.util.*;
import java.util.stream.Collectors;

public class ErrorCollector implements Iterable<ErrorMessage> {

  @Getter
  private final ErrorEmitter errorEmitter;
  private final List<ErrorMessage> errors;

  private final Map<Class, ErrorHandler> handlers = new HashMap<>();

  private ErrorCollector(@NonNull ErrorEmitter errorEmitter, @NonNull List<ErrorMessage> errors) {
    this.errorEmitter = errorEmitter;
    this.errors = errors;
  }

  private ErrorCollector(@NonNull ErrorLocation location, @NonNull List<ErrorMessage> errors) {
    this(new ErrorEmitter(location), errors);
  }

  private ErrorCollector(@NonNull ErrorLocation location) {
    this(location, new ArrayList<>(5));
  }

  public static ErrorCollector root() {
    return new ErrorCollector(ErrorPrefix.ROOT);
  }

  public void registerHandler(Class clazz, ErrorHandler handler) {
//      SqrlAstException.class, new SqrlAstExceptionHandler(),
//      ValidationException.class, new ValidationExceptionHandler()
    handlers.put(clazz, handler);
  }

  public static ErrorCollector fromPrefix(@NonNull ErrorPrefix prefix) {
    return new ErrorCollector(prefix);
  }

  public ErrorCollector resolve(String location) {
    return new ErrorCollector(errorEmitter.resolve(location), errors);
  }

  public ErrorCollector resolve(Name location) {
    return new ErrorCollector(errorEmitter.resolve(location), errors);
  }

  protected void addInternal(@NonNull ErrorMessage error) {
    errors.add(error);
  }

  protected void add(@NonNull ErrorMessage err) {
    ErrorLocation errLoc = err.getLocation();
    if (!errLoc.hasPrefix()) {
      //Adjust relative location
      ErrorLocation newloc = errorEmitter.getBaseLocation().append(errLoc);
      err = new ErrorMessage.Implementation(err.getMessage(), newloc, err.getSeverity());
    }
    addInternal(err);
  }

  protected void addAll(ErrorCollector other) {
    if (other == null) {
      return;
    }
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
    addInternal(errorEmitter.fatal(msg, args));
  }

  public void fatal(int line, int offset, String msg, Object... args) {
    addInternal(errorEmitter.fatal(line, offset, msg, args));
  }

  public void warn(String msg, Object... args) {
    addInternal(errorEmitter.warn(msg, args));
  }

  public void warn(int line, int offset, String msg, Object... args) {
    addInternal(errorEmitter.warn(line, offset, msg, args));
  }


  public void notice(String msg, Object... args) {
    addInternal(errorEmitter.notice(msg, args));
  }

  public void notice(int line, int offset, String msg, Object... args) {
    addInternal(errorEmitter.notice(line, offset, msg, args));
  }

  @Override
  public Iterator<ErrorMessage> iterator() {
    return errors.iterator();
  }

  public String combineMessages(ErrorMessage.Severity minSeverity, String prefix,
      String delimiter) {
    String suffix = "";
    if (errors != null) {
      suffix = errors.stream().filter(m -> m.getSeverity().compareTo(minSeverity) >= 0)
          .map(ErrorMessage::toString)
          .collect(Collectors.joining(delimiter));
    }
    return prefix + suffix;
  }

  @Override
  public String toString() {
    return combineMessages(ErrorMessage.Severity.NOTICE, "", "\n");
  }

  public List<ErrorMessage> getAll() {
    return new ArrayList<>(errors);
  }

//  public void log() {
//    for (ErrorMessage message : errors) {
//      if (message.isNotice()) {
//        log.info(message.toStringNoSeverity());
//      } else if (message.isWarning()) {
//        log.warn(message.toStringNoSeverity());
//      } else if (message.isFatal()) {
//        log.error(message.toStringNoSeverity());
//      } else {
//        throw new UnsupportedOperationException("Unexpected severity: " + message.getSeverity());
//      }
//    }
//  }

  public void handle(Exception e) {
    Optional<ErrorHandler> handler = Optional.ofNullable(handlers.get(e.getClass()));
    handler.ifPresentOrElse(
        h -> add(h.handle(e, errorEmitter)),
        () -> fatal(e.getMessage()));
  }
}
