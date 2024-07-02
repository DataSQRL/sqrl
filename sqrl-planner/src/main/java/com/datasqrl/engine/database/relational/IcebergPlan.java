package com.datasqrl.engine.database.relational;

import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.sql.SqlDDLStatement;
import java.util.List;
import java.util.Map;
import lombok.Value;

@Value
public class IcebergPlan implements EnginePhysicalPlan {
  List<IcebergSerializableTable> sinks;
  List<SqlDDLStatement> ddl;
  Map<String, EnginePhysicalPlan> queryEnginePlans;


  @Value
  public static class IcebergSerializableTable {
    //todo make list
    String namespace;
    String name;
    List<IcebergSerializableColumn> columns;
  }

  @Value
  public static class IcebergSerializableColumn {
    boolean optional;
    int index;
    String name;
    String typeName;
  }

}
