package com.datasqrl.engine.log.postgres;

import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.sql.SqlDDLStatement;
import java.util.ArrayList;
import java.util.List;
import lombok.Value;

@Value
public class PostgresPhysicalPlan implements EnginePhysicalPlan {
  List<SqlDDLStatement> ddl = new ArrayList<>();
  List<SqlDDLStatement> listenQueries = new ArrayList<>();
  List<SqlDDLStatement> onNotifyQueries = new ArrayList<>();
}
