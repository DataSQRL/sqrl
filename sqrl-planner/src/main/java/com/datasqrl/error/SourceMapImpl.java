/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
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

import com.datasqrl.error.ErrorLocation.FileRange;
import lombok.Value;

@Value
public class SourceMapImpl implements SourceMap {

  String source;

  @Override
  public String getRange(FileRange range) {
    var src = source.split("\n");
    var result = new StringBuilder();
    for (var i = range.fromLine() - 1; i < range.toLine(); i++) {
      var line = src[Math.min(i, src.length - 1)];
      if (i == range.toLine() - 1) { // last line, substring to toOffset
        line = line.substring(0, Math.min(line.length(), range.toOffset()));
      }
      if (i == range.fromLine() - 1) { // first line, substring to fromOffset
        //        Preconditions.checkArgument(range.getFromOffset()<line.length(),
        //            "Invalid offset [%s] for: %s", range.getFromOffset(), line);
        //        line = line.substring(range.getFromOffset()-1,line.length());
      }
      result.append(line).append("\n");
    }
    return result.toString();
  }
}
