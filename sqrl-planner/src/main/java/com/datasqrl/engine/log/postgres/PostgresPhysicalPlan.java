package com.datasqrl.engine.log.postgres;

import com.datasqrl.config.EngineFactory.Type;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.database.relational.ddl.statements.InsertStatement;
import com.datasqrl.engine.database.relational.ddl.statements.notify.ListenNotifyAssets;
import com.datasqrl.sql.SqlDDLStatement;
import java.util.ArrayList;
import java.util.List;
import lombok.Value;

@Value
public class PostgresPhysicalPlan implements EnginePhysicalPlan {
  List<SqlDDLStatement> ddl = new ArrayList<>();
  List<ListenNotifyAssets> queries = new ArrayList<>();
  List<InsertStatement> inserts = new ArrayList<>();

  @Override
  public Type getType() {
    return Type.LOG;
  }
}
