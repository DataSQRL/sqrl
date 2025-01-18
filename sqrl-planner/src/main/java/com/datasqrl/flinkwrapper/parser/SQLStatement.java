package com.datasqrl.flinkwrapper.parser;

import com.datasqrl.error.ErrorLocation.FileLocation;

public interface SQLStatement {

  default FileLocation mapSqlLocation(FileLocation location) {
    return location;
  }

  static FileLocation removeFirstRowOffset(FileLocation location, int firstRowOffset) {
    if (location.getLine()==1) {
      return new FileLocation(1, Math.max(location.getOffset()-firstRowOffset,1));
    }
    return location;
  }

}
