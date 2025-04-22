package com.datasqrl.engine.log.postgres;

import java.util.List;

import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.database.relational.ddl.statements.InsertStatement;
import com.datasqrl.engine.database.relational.ddl.statements.notify.ListenNotifyAssets;
import com.datasqrl.sql.SqlDDLStatement;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class PostgresLogPhysicalPlan implements EnginePhysicalPlan {
  List<SqlDDLStatement> ddl;
  List<ListenNotifyAssets> queries;
  List<InsertStatement> inserts;
}
