/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.database.relational;

import com.datasqrl.engine.database.DatabaseViewPhysicalPlan;
import com.datasqrl.engine.database.QueryTemplate;
import com.datasqrl.sql.SqlDDLStatement;
import com.datasqrl.plan.queries.IdentifiedQuery;
import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.List;
import java.util.Map;
import lombok.Value;

@Value
@Deprecated
public class JDBCPhysicalPlanOld implements DatabaseViewPhysicalPlan {

  List<SqlDDLStatement> ddl;
  List<DatabaseView> views;

  //todo remove
  @JsonIgnore
  Map<IdentifiedQuery, QueryTemplate> queryPlans;
}
