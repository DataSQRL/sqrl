/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.error;

import com.datasqrl.error.ErrorLocation.FileRange;
import lombok.Value;

@Value
public class SourceMapImpl implements SourceMap {

  String source;

  @Override
  public String getRange(FileRange range) {
    String[] src = source.split("\n");
    StringBuilder result = new StringBuilder();
    for (int i = range.getFromLine()-1; i < range.getToLine(); i++) {
      String line = src[Math.min(i, src.length - 1)];
      if (i==range.getToLine()-1) { //last line, substring to toOffset
        line = line.substring(0,Math.min(line.length(),range.getToOffset()));
      }
      if (i==range.getFromLine()-1) {//first line, substring to fromOffset
//        Preconditions.checkArgument(range.getFromOffset()<line.length(),
//            "Invalid offset [%s] for: %s", range.getFromOffset(), line);
//        line = line.substring(range.getFromOffset()-1,line.length());
      }
      result.append(line).append("\n");
    }
    return result.toString();
  }
}
