/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.database;

import java.util.List;
import java.util.Map;

import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.database.relational.ddl.statements.CreateIndexDDL;
import com.datasqrl.engine.database.relational.ddl.statements.DropIndexDDL;
import com.datasqrl.plan.queries.IdentifiedQuery;
import com.datasqrl.sql.SqlDDLStatement;

public interface DatabasePhysicalPlan extends EnginePhysicalPlan {

  List<SqlDDLStatement> getDdl();

  default boolean removeIndexDdl() {
    return getDdl().removeIf(ddl -> (ddl instanceof CreateIndexDDL) || (ddl instanceof DropIndexDDL));
  }

  Map<IdentifiedQuery, QueryTemplate> getQueryPlans();

}
