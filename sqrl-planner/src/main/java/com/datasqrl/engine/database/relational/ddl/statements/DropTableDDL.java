/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.database.relational.ddl.statements;

import com.datasqrl.sql.SqlDDLStatement;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class DropTableDDL implements SqlDDLStatement {

  String name;

  @Override
  public String getSql() {
    return "DROP TABLE IF EXISTS " + name + ";";
  }
}
