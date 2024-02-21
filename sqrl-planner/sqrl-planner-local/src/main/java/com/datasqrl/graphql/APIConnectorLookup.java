package com.datasqrl.graphql;

import com.datasqrl.engine.log.Log;
import com.datasqrl.graphql.inference.SqrlSchemaForInference;
import com.datasqrl.plan.queries.APIQuery;

import java.util.List;
import java.util.Map;

public interface APIConnectorLookup {

    List<Log> getLogs();

    List<APIQuery> getQueries();

    Map<SqrlSchemaForInference.SQRLTable, Log> getExports();

}
