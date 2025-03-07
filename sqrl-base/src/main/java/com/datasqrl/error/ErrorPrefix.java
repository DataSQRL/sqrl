/*
 * Copyright © 2024 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.error;

import lombok.NonNull;

public enum ErrorPrefix implements ErrorLocation {
  ROOT,
  SCRIPT,
  SCHEMA,
  CONFIG,
  INPUT_DATA;

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
    String res = getPrefix();
    if (res == null) {
      return "";
    } else {
      return res;
    }
  }
}
