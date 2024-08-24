/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.database;

import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.plan.queries.APIQuery;

import com.datasqrl.plan.queries.IdentifiedQuery;
import com.datasqrl.sql.SqlDDLStatement;
import java.util.List;
import java.util.Map;

public interface DatabasePhysicalPlan extends EnginePhysicalPlan {

  List<SqlDDLStatement> getDdl();

  Map<IdentifiedQuery, QueryTemplate> getQueryPlans();

}
