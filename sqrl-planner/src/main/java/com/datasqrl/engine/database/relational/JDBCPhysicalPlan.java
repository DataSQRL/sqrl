/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.database.relational;

import com.datasqrl.engine.database.DatabasePhysicalPlan;
import com.datasqrl.engine.database.QueryTemplate;
import com.datasqrl.sql.SqlDDLStatement;
import com.datasqrl.plan.queries.IdentifiedQuery;
import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.List;
import java.util.Map;
import lombok.Value;

@Value
public class JDBCPhysicalPlan implements DatabasePhysicalPlan {

  List<SqlDDLStatement> ddl;
  //todo fix me to include queries
  @JsonIgnore
  Map<IdentifiedQuery, QueryTemplate> queries;
}
