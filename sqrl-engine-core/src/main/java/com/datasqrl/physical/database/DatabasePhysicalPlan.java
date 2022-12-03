package com.datasqrl.physical.database;

import com.datasqrl.physical.EnginePhysicalPlan;
import com.datasqrl.plan.queries.APIQuery;

import java.util.Map;

public interface DatabasePhysicalPlan extends EnginePhysicalPlan {

    Map<APIQuery, QueryTemplate> getQueries();

}
