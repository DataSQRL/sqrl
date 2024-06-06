package com.datasqrl.engine.log.postgres;

import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.sql.SqlDDLStatement;
import java.util.List;
import lombok.Value;

@Value
public class PostgresPhysicalPlan implements EnginePhysicalPlan {
  List<SqlDDLStatement> ddl;
}
