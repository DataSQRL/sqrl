/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.error;

import lombok.NonNull;

public enum ErrorPrefix implements ErrorLocation {

  ROOT, SCRIPT, SCHEMA, CONFIG, INPUT_DATA;

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
  public ErrorLocation withSourceMap(SourceMap map) {
    return ErrorLocationImpl.of(getPrefix(), map);
  }

  @Override
  public SourceMap getSourceMap() {
    return null;
  }

  @Override
  public FileRange getFile() {
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
  public ErrorLocation atFile(@NonNull ErrorLocation.FileRange file) {
    throw new IllegalArgumentException("Need to set source map first");
  }

  @Override
  public String toString() {
    var res = getPrefix();
    if (res == null) {
      return "";
    } else {
      return res;
    }
  }

}
