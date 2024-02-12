package com.datasqrl.graphql;

import com.datasqrl.calcite.function.SqrlTableMacro;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.engine.log.Log;
import com.datasqrl.graphql.APIConnectorManagerImpl.LogModule;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.plan.queries.APIMutation;
import com.datasqrl.plan.queries.APIQuery;
import com.datasqrl.plan.queries.APISubscription;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ApiConnections {

  private final Map<APIMutation, TableSink> mutations = new HashMap<>();

  private final Map<NamePath, LogModule> modules = new HashMap<>();

  private final Map<APISubscription, TableSource> subscriptions = new HashMap<>();

  private final Map<SqrlTableMacro, Log> exports = new HashMap<>();

  private final List<APIQuery> queries = new ArrayList<>();
}
