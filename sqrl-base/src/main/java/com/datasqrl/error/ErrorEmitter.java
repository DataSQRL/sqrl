/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.error;

import com.datasqrl.name.Name;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;

public class ErrorEmitter {

  protected final Map<Class, ErrorHandler> handlers = new HashMap<>();

  @Getter
  private final SourceMap sourceMap;

  @Getter
  private final ErrorLocation baseLocation;

  public ErrorEmitter(SourceMap sourceMap, ErrorLocation baseLocation) {
    this.sourceMap = sourceMap;
    this.baseLocation = baseLocation;
  }

  public ErrorEmitter resolve(Name location) {
    return new ErrorEmitter(sourceMap, baseLocation.resolve(location));
  }

  public ErrorEmitter resolve(String location) {
    return new ErrorEmitter(sourceMap, baseLocation.resolve(location));
  }

  public ErrorEmitter resolveSourceMap(SourceMap sourceMap) {
    return new ErrorEmitter(sourceMap, baseLocation);
  }

  public ErrorMessage fatal(String msg, Object... args) {
    return new ErrorMessage.Implementation(getMessage(msg, args), baseLocation,
        ErrorMessage.Severity.FATAL, sourceMap);
  }

  public ErrorMessage fatal(int line, int offset, String msg, Object... args) {
    return new ErrorMessage.Implementation(getMessage(msg, args), baseLocation.atFile(line, offset),
        ErrorMessage.Severity.FATAL, sourceMap);
  }

  public ErrorMessage warn(String msg, Object... args) {
    return new ErrorMessage.Implementation(getMessage(msg, args), baseLocation,
        ErrorMessage.Severity.WARN, sourceMap);
  }

  public ErrorMessage warn(int line, int offset, String msg, Object... args) {
    return new ErrorMessage.Implementation(getMessage(msg, args), baseLocation.atFile(line, offset),
        ErrorMessage.Severity.WARN, sourceMap);
  }

  public ErrorMessage notice(String msg, Object... args) {
    return new ErrorMessage.Implementation(getMessage(msg, args), baseLocation,
        ErrorMessage.Severity.NOTICE, sourceMap);
  }

  public ErrorMessage notice(int line, int offset, String msg, Object... args) {
    return new ErrorMessage.Implementation(getMessage(msg, args), baseLocation.atFile(line, offset),
        ErrorMessage.Severity.NOTICE, sourceMap);
  }

  private static String getMessage(String msgTemplate, Object... args) {
    if (args == null || args.length == 0) {
      return msgTemplate;
    }
    return String.format(msgTemplate, args);
  }

  public Optional<ErrorMessage> handle(Exception e) {
    Optional<ErrorHandler> handler = Optional.ofNullable(handlers.get(e.getClass()));
    return handler.map(
        h -> h.handle(e, this));
  }
}
