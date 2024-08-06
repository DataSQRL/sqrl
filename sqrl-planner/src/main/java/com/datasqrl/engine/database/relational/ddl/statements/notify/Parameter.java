package com.datasqrl.engine.database.relational.ddl.statements.notify;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.rel.type.RelDataTypeField;

@Getter
@AllArgsConstructor
public class Parameter {
  String name;
  RelDataTypeField relDataTypeField;
}
