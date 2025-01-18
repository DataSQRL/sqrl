package com.datasqrl.graphql;

import com.datasqrl.calcite.function.SqrlTableMacro;
import com.datasqrl.engine.log.Log;
import com.datasqrl.plan.queries.APIQuery;

import java.util.List;
import java.util.Map;

public interface APIConnectorLookup {

    List<Log> getLogs();

    List<APIQuery> getQueries();

    Map<SqrlTableMacro, Log> getExports();

}
