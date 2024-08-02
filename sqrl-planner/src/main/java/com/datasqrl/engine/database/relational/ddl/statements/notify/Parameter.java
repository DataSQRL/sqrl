package com.datasqrl.engine.database.relational.ddl.statements.notify;

import com.datasqrl.engine.database.relational.ddl.PostgresDDLFactory;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.rel.type.RelDataTypeField;

@Getter
@AllArgsConstructor
public class Parameter {
  String name;
  @JsonIgnore
  RelDataTypeField relDataTypeField;

  public String getType() {
    return PostgresDDLFactory.getSqlType(relDataTypeField);
  }
}
