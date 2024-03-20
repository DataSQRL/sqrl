/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.error;

import com.datasqrl.error.ErrorLocation.FileLocation;
import com.datasqrl.error.ErrorLocation.FileRange;
import com.datasqrl.error.ErrorMessage.Implementation;
import com.datasqrl.error.ErrorMessage.Severity;
import java.io.Serializable;
import java.net.URI;
import java.nio.file.Path;
import java.util.Iterator;
import lombok.Getter;
import lombok.NonNull;

/**
 * Proxy class for {@link ErrorLocation} and {@link ErrorCollection} for usability.
 */
public class ErrorCollector implements Iterable<ErrorMessage>, Serializable {

  private static final boolean DEFAULT_ABORT_ON_FATAL = true;

  @Getter
  private final ErrorLocation location;
  private final ErrorCollection errors;
  private final boolean abortOnFatal;

  /*
  ==== CONSTRUCTORS ====
   */

  private ErrorCollector(ErrorLocation location, ErrorCollection errors, boolean abortOnFatal) {
    this.location = location;
    this.errors = errors;
    this.abortOnFatal = abortOnFatal;
  }

  public ErrorCollector(ErrorLocation location, ErrorCollection errors) {
    this(location, errors, DEFAULT_ABORT_ON_FATAL);
  }

  public ErrorCollector(@NonNull ErrorLocation location) {
    this(location, new ErrorCollection());
  }

  public static ErrorCollector root() {
    return new ErrorCollector(ErrorPrefix.ROOT);
  }

  public ErrorCollector fromPrefix(@NonNull ErrorPrefix prefix) {
    return new ErrorCollector(prefix, errors, abortOnFatal);
  }

  public ErrorCollector abortOnFatal(boolean abortOnFatal) {
    return new ErrorCollector(location, errors, abortOnFatal);
  }

  /*
  ==== Proxies ErrorLocation for convenience ====
   */

  public ErrorLocation getLocation() {
    return location;
  }

  public ErrorCollector withSource(SourceMap sourceMap) {
    return new ErrorCollector(location.withSourceMap(sourceMap), errors, abortOnFatal);
  }

  public ErrorCollector withSource(String sourceContent) {
    return withSource(new SourceMapImpl(sourceContent));
  }

  public ErrorCollector resolve(String sub) {
    return new ErrorCollector(location.resolve(sub), errors, abortOnFatal);
  }

//  public ErrorCollector resolve(Name sub) {
//    return new ErrorCollector(location.resolve(sub), errors, abortOnFatal);
//  }

  public ErrorCollector atFile(FileRange file) {
    return new ErrorCollector(location.atFile(file), errors, abortOnFatal);
  }

  public ErrorCollector atFile(FileLocation file) {
    return new ErrorCollector(location.atFile(file), errors, abortOnFatal);
  }

  public ErrorCollector withLocation(ErrorLocation location) {
    return new ErrorCollector(location, errors, abortOnFatal);
  }

  public ErrorCollector withScript(Path file, String scriptContent) {
    return withScript(file.getFileName().toString(),scriptContent);
  }

  public ErrorCollector withScript(String filename, String scriptContent) {
    return withLocation(ErrorPrefix.SCRIPT.resolve(filename)).withSource(scriptContent);
  }

  public ErrorCollector withConfig(Path file) {
    return withConfig(file.getFileName().toString());
  }

  public ErrorCollector withConfig(URI uri) {
    return withConfig(uri.getPath());
  }

  public ErrorCollector withConfig(String filename) {
    return withLocation(ErrorPrefix.CONFIG.resolve(filename));
  }

  /*
  ==== Proxies ErrorCollection for convenience ====
   */

  public boolean hasErrors() {
    return errors.hasErrors();
  }

  public boolean hasErrorsWarningsOrNotices() {
    return errors.hasErrorsWarningsOrNotices();
  }

  public boolean isFatal() {
    return hasErrors();
  }

  @Override
  public Iterator<ErrorMessage> iterator() {
    return errors.iterator();
  }

  @Override
  public String toString() {
    return errors.toString();
  }

  public ErrorCollection getErrors() {
    return errors;
  }

  public ErrorCatcher getCatcher() {
    return new ErrorCatcher(location, errors);
  }

  public RuntimeException handle(Throwable e) {
    return getCatcher().handle(e);
  }

  /*
  ==== Factory methods for creating errors ====
   */

  protected void addInternal(@NonNull ErrorMessage error) {
    errors.addInternal(error);
  }

  public void fatal(String msg, Object... args) {
    fatal(ErrorLabel.GENERIC,msg,args);
  }

  public void fatal(ErrorLabel label, String msg, Object... args) {
    RuntimeException exception = exception(label, msg, args);
    if (abortOnFatal) {
      throw exception;
    }
  }

  public RuntimeException exception(String msg, Object... args) {
    return exception(ErrorLabel.GENERIC,msg,args);
  }

  public RuntimeException exception(ErrorLabel label, String msg, Object... args) {
    ErrorMessage errorMessage = new Implementation(label, ErrorMessage.getMessage(msg,args), location, Severity.FATAL);
    addInternal(errorMessage);
    return new CollectedException(errorMessage.asException());
  }

  public void checkFatal(boolean condition, String msg, Object... args) {
    if (!condition) {
      fatal(ErrorLabel.GENERIC,msg,args);
    }
  }

  public void checkFatal(boolean condition, ErrorLabel label, String msg, Object... args) {
    if (!condition) {
      fatal(label,msg,args);
    }
  }

  public void warn(String msg, Object... args) {
    warn(ErrorLabel.GENERIC, msg, args);
  }

  public void warn(ErrorLabel label, String msg, Object... args) {
    ErrorMessage errorMessage = new Implementation(label, ErrorMessage.getMessage(msg,args), location, Severity.WARN);
    addInternal(errorMessage);
  }

  public void notice(String msg, Object... args) {
    notice(ErrorLabel.GENERIC,msg,args);
  }

  public void notice(ErrorLabel label, String msg, Object... args) {
    ErrorMessage errorMessage = new Implementation(label, ErrorMessage.getMessage(msg,args), location, Severity.NOTICE);
    addInternal(errorMessage);
  }

  public ErrorCollector withSchema(String name, String schema) {
    return withLocation(ErrorPrefix.SCRIPT
        .resolve(name)).withSource(schema);
  }
}
