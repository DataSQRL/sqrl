package ai.datasqrl.physical.database;

import ai.datasqrl.physical.EnginePhysicalPlan;
import ai.datasqrl.plan.queries.APIQuery;

import java.util.Map;

public interface DatabasePhysicalPlan extends EnginePhysicalPlan {

    Map<APIQuery, QueryTemplate> getQueries();

}
