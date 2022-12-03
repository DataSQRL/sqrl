package com.datasqrl.engine.database;

import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.plan.queries.APIQuery;

import java.util.Map;

public interface DatabasePhysicalPlan extends EnginePhysicalPlan {

    Map<APIQuery, QueryTemplate> getQueries();

}
