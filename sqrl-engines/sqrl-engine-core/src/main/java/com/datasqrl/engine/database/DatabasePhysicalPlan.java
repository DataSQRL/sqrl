/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.database;

import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.plan.queries.APIQuery;

import java.util.Map;

public interface DatabasePhysicalPlan extends EnginePhysicalPlan {

  Map<APIQuery, QueryTemplate> getQueries();

}
