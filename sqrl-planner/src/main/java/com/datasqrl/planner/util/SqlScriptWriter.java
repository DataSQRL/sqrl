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
package com.datasqrl.planner.util;

import java.util.Collection;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang3.StringUtils;

public final class SqlScriptWriter {

  private static final String SEMICOLON = ";";

  private final StringBuilder scriptBuilder = new StringBuilder();

  public void append(String stmt) {
    if (StringUtils.isBlank(stmt)) {
      return;
    }

    String formattedStmt;
    if (stmt.endsWith(SEMICOLON)) {
      formattedStmt = stmt + "\n";

    } else if (stmt.trim().endsWith(SEMICOLON)) {
      formattedStmt = stmt; // do nothing

    } else {
      formattedStmt = stmt + ";\n";
    }

    scriptBuilder.append(formattedStmt);
  }

  public void append(SqlNode sqlNode) {
    append(sqlNode.toString());
  }

  public void append(Collection<String> stmts) {
    if (stmts != null) {
      stmts.forEach(this::append);
    }
  }

  @Override
  public String toString() {
    return scriptBuilder.toString();
  }
}
