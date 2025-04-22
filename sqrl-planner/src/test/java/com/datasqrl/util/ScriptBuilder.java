/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.util;

import java.util.ArrayList;
import java.util.List;

import com.datasqrl.canonicalizer.NamePath;

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
