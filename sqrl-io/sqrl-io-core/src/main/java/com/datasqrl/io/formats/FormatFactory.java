/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.formats;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.io.impl.InputPreview;
import com.datasqrl.util.ServiceLoaderDiscovery;
import java.io.Serializable;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import lombok.NonNull;
import lombok.Value;

public interface FormatFactory {

  List<String> getExtensions();

  String getName();

  Parser getParser(@NonNull SqrlConfig config);

  interface Parser extends Serializable {

    @Value
    class Result {

      Type type;
      Map<String, Object> record;
      Instant sourceTime;
      String errorMsg;

      public static Result error(String msg) {
        return new Result(Type.ERROR, null, null, msg);
      }

      public static Result success(@NonNull Map<String, Object> record) {
        return new Result(Type.SUCCESS, record, null, null);
      }

      public static Result success(@NonNull Map<String, Object> record, @NonNull Instant time) {
        return new Result(Type.SUCCESS, record, time, null);
      }

      public static Result skip() {
        return new Result(Type.SKIP, null, null, null);
      }

      public boolean isSuccess() {
        return type == Type.SUCCESS;
      }

      public boolean isError() { return type == Type.ERROR; }

      public boolean isSkip() { return type == Type.SKIP; }

      public boolean hasTime() {
        return sourceTime != null;
      }

      public enum Type {ERROR, SKIP, SUCCESS}

    }

  }

  /**
   * If the format configuration is not complete, this method attempts to complete the configuration
   * by reading and analyzing input data from the provided {@link InputPreview}.
   *
   * @param config
   * @param inputPreview
   */
  default void inferConfig(@NonNull SqrlConfig config, @NonNull InputPreview inputPreview) {

  }

  Writer getWriter(@NonNull SqrlConfig config);

  interface Writer {

  }

  public static final String FORMAT_NAME_KEY = "name";
  static FormatFactory fromConfig(@NonNull SqrlConfig config) {
    return ServiceLoaderDiscovery.get(FormatFactory.class, FormatFactory::getName,
        config.asString(FORMAT_NAME_KEY).get());
  }

}
