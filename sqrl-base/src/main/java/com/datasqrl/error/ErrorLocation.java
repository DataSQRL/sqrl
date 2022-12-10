/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.error;

import com.datasqrl.name.Name;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.io.Serializable;
import lombok.NonNull;
import lombok.Value;

@JsonSerialize(as = ErrorLocation.class)
public interface ErrorLocation extends Serializable {

  String getPrefix();

  @JsonIgnore
  default boolean hasPrefix() {
    return !Strings.isNullOrEmpty(getPrefix());
  }

  @JsonIgnore
  @NonNull String[] getPathArray();

  @JsonIgnore
  default String getPathAt(int index) {
    return getPathArray()[index];
  }

  @JsonIgnore
  default int getPathLength() {
    return getPathArray().length;
  }

  String getPath();

  FileLocation getFile();

  @JsonIgnore
  default boolean hasFile() {
    return getFile() != null;
  }

  ErrorLocation append(@NonNull ErrorLocation other);

  ErrorLocation resolve(@NonNull String location);

  default ErrorLocation resolve(@NonNull Name location) {
    return resolve(location.getDisplay());
  }

  default ErrorLocation atFile(int line, int offset) {
    Preconditions.checkArgument(line > 0 && offset > 0);
    return atFile(new FileLocation(line, offset));
  }

  ErrorLocation atFile(@NonNull ErrorLocation.FileLocation file);

  @Value
  class FileLocation {

    private final int line;
    private final int offset;

  }

}