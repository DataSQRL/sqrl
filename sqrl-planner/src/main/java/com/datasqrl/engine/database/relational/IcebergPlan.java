package com.datasqrl.engine.database.relational;

import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.sql.SqlDDLStatement;
import java.util.List;
import java.util.Map;
import lombok.Value;

@Value
public class IcebergPlan implements EnginePhysicalPlan {

  EnginePhysicalPlan plan;

}
