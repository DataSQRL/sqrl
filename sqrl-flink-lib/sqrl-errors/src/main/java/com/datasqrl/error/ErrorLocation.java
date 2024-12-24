/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.error;

import java.io.Serializable;
import lombok.NonNull;
import lombok.Value;

//@JsonSerialize(as = ErrorLocation.class)
public interface ErrorLocation extends Serializable {

  String getPrefix();

  @NonNull String[] getPathArray();

  FileRange getFile();

  default boolean hasPrefix() {
    return !(getPrefix() == null || getPrefix().trim().isEmpty());
  }

  default String getPathAt(int index) {
    return getPathArray()[index];
  }

  default int getPathLength() {
    return getPathArray().length;
  }

  String getPath();

  default boolean hasFile() {
    return getFile() != null;
  }

  default FileLocation getFileLocation() {
    return getFile().asLocation();
  }

  ErrorLocation withSourceMap(SourceMap map);

  SourceMap getSourceMap();

  ErrorLocation append(@NonNull ErrorLocation other);

  ErrorLocation resolve(@NonNull String location);

//  default ErrorLocation resolve(@NonNull Name location) {
//    return resolve(location.getDisplay());
//  }
  
  default ErrorLocation atFile(@NonNull ErrorLocation.FileLocation file) {
    return atFile(new FileRange(file));
  }

  ErrorLocation atFile(@NonNull ErrorLocation.FileRange file);

  @Value
  class FileLocation {

    public static final FileLocation START = new FileLocation(1,1);

    private final int line;
    private final int offset;

    public FileLocation add(FileLocation additional) {
      if (additional.getLine()==1) {
        return new FileLocation(line, offset + additional.offset-1);
      } else {
        return new FileLocation(line + additional.line-1, additional.offset);
      }
    }

  }

  @Value
  class FileRange {
    private final int fromLine;
    private final int toLine;
    private final int fromOffset;
    private final int toOffset;

    public FileRange(FileLocation location) {
      this(location.line, location.offset, location.line, location.offset);
    }

    public FileRange(int fromLine, int fromOffset, int toLine, int toOffset) {
      this.fromLine = fromLine;
      this.toLine = toLine;
      this.fromOffset = fromOffset;
      this.toOffset = toOffset;
//      Preconditions.checkArgument(fromLine>0 && toLine>0 && fromOffset>0 && toOffset>0, "Invalid file: %s",this);
//      Preconditions.checkArgument(fromLine<=toLine && (fromLine!=toLine || fromOffset<=toOffset), "Invalid file: %s",this);
    }

    public boolean isLocation() {
      return toLine==fromLine && fromOffset==toOffset;
    }

    public FileLocation asLocation() {
//      Preconditions.checkArgument(isLocation());
      return new FileLocation(fromLine, fromOffset);
    }

    @Override
    public String toString() {
      String result = fromLine+":"+fromOffset;
      if (!isLocation()) {
        result += "-" + toLine+":"+toOffset;
      }
      return result;
    }

  }

}