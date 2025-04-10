package com.datasqrl.v2.parser;

import com.datasqrl.error.ErrorLocation.FileLocation;

/**
 * Most generic interface for parsed statements
 */
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
