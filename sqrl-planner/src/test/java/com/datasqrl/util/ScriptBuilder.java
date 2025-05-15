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
package com.datasqrl.util;

import com.datasqrl.canonicalizer.NamePath;
import java.util.ArrayList;
import java.util.List;
import lombok.Value;

@Value
public class ScriptBuilder {

  StringBuilder s = new StringBuilder();
  List<String> tables = new ArrayList<>();

  public ScriptBuilder add(String statement) {
    s.append(statement);
    if (!statement.endsWith(";\n")) {
      if (statement.endsWith(";")) {
        s.append("\n");
      } else {
        s.append(";\n");
      }
    }
    return this;
  }

  public ScriptBuilder add(String tblName, String statement) {
    add(tblName + " := " + statement);
    return with(tblName);
  }

  private String getTableId(String tblName) {
    return NamePath.parse(tblName).getLast().getDisplay();
  }

  public ScriptBuilder with(String... tblNames) {
    for (String tblName : tblNames) {
      tables.add(getTableId(tblName));
    }
    return this;
  }

  public ScriptBuilder append(String statement) {
    return add(statement);
  }

  public String getScript() {
    return toString();
  }

  @Override
  public String toString() {
    return s.toString();
  }

  public static ScriptBuilder of(String... statements) {
    var s = new ScriptBuilder();
    for (String statement : statements) {
      s.add(statement);
    }
    return s;
  }
}
