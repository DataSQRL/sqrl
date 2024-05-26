/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.database;

import com.datasqrl.engine.EnginePhysicalPlan;

import com.datasqrl.plan.queries.IdentifiedQuery;
import java.util.Map;

public interface DatabasePhysicalPlan extends EnginePhysicalPlan {

  Map<IdentifiedQuery, QueryTemplate> getQueries();

}
