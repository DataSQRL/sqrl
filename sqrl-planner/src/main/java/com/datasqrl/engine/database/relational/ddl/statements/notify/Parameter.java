package com.datasqrl.engine.database.relational.ddl.statements.notify;

import org.apache.calcite.rel.type.RelDataTypeField;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class Parameter {
  String name;
  RelDataTypeField relDataTypeField;
}
