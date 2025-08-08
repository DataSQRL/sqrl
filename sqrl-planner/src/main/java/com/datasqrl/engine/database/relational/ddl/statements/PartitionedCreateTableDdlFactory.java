package com.datasqrl.engine.database.relational.ddl.statements;

import com.datasqrl.engine.database.relational.CreateTableJdbcStatement;
import com.datasqrl.engine.database.relational.CreateTableJdbcStatement.PartitionType;

abstract class PartitionedCreateTableDdlFactory extends GenericCreateTableDdlFactory {

  @Override
  public String createTableDdl(CreateTableJdbcStatement statement) {
    var ddl = super.createTableDdl(statement);

    if (statement.getPartitionType() == PartitionType.NONE) {
      return ddl;
    }

    validatePartitionType(statement.getPartitionType());

    return "%s PARTITIONED BY (%s)".formatted(ddl, listToSql(statement.getPartitionKey()));
  }

  abstract void validatePartitionType(PartitionType partitionType);
}
