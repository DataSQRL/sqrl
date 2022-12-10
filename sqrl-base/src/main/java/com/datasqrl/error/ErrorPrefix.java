/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.error;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.NonNull;

@JsonSerialize(as = ErrorLocation.class)
public enum ErrorPrefix implements ErrorLocation {

  ROOT, SOURCE, SINK, ENGINE, SCRIPT, INITIALIZE, INPUT_DATA;

  @Override
  public String getPrefix() {
    if (this == ROOT) {
      return null;
    }
    return name().toLowerCase();
  }

  private static final String[] EMPTY_PATH = new String[0];

  @Override
  public @NonNull String[] getPathArray() {
    return EMPTY_PATH;
  }

  @Override
  public String getPath() {
    return "";
  }

  @Override
  public FileLocation getFile() {
    return null;
  }

  @Override
  public ErrorLocation append(@NonNull ErrorLocation other) {
    return ErrorLocationImpl.of(getPrefix(), other);
  }

  @Override
  public ErrorLocation resolve(@NonNull String location) {
    return ErrorLocationImpl.of(getPrefix(), location);
  }

  @Override
  public ErrorLocation atFile(@NonNull ErrorLocation.FileLocation file) {
    return ErrorLocationImpl.of(getPrefix(), file);
  }

  @Override
  public String toString() {
    String res = getPrefix();
    if (res == null) {
      return "";
    } else {
      return res;
    }
  }

}
