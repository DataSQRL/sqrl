/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.error;

import com.datasqrl.error.SourceMap.EmptySourceMap;
import com.datasqrl.name.Name;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.NonNull;

public class ErrorCollector implements Iterable<ErrorMessage> {

  @Getter
  private final ErrorEmitter errorEmitter;
  private final List<ErrorMessage> errors;

  private ErrorCollector(@NonNull ErrorEmitter errorEmitter, @NonNull List<ErrorMessage> errors) {
    this.errorEmitter = errorEmitter;
    this.errors = errors;
    registerHandlers();
  }

  private ErrorCollector(@NonNull ErrorLocation location, @NonNull List<ErrorMessage> errors) {
    this(new ErrorEmitter(new EmptySourceMap(), location), errors);
  }

  public ErrorCollector(@NonNull ErrorLocation location) {
    this(location, new ArrayList<>(5));
  }

  private void registerHandlers() {
    ServiceLoader<ErrorHandler> serviceLoader = ServiceLoader.load(ErrorHandler.class);
    for (ErrorHandler handler : serviceLoader) {
      registerHandler(handler.getHandleClass(), handler);
    }
  }

  public static ErrorCollector root() {
    return new ErrorCollector(ErrorPrefix.ROOT);
  }

  public void registerHandler(Class clazz, ErrorHandler handler) {
    errorEmitter.handlers.put(clazz, handler);
  }

  public ErrorCollector fromPrefix(@NonNull ErrorPrefix prefix) {
    return new ErrorCollector(prefix, errors);
  }

  public ErrorCollector sourceMap(SourceMap sourceMap) {
    return new ErrorCollector(errorEmitter.resolveSourceMap(sourceMap), errors);
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
      err = new ErrorMessage.Implementation(err.getErrorCode(), err.getMessage(), newloc,
          err.getSeverity(),
          errorEmitter.getSourceMap());
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
    Optional<ErrorMessage> handler = errorEmitter.handle(e);
    handler.ifPresentOrElse(
        this::add,
        () -> fatal(e.getMessage()));
  }
}
