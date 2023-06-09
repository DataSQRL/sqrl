package com.datasqrl.plan.local.analyze;

import com.datasqrl.engine.log.Log;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.plan.queries.APIMutation;
import com.datasqrl.plan.queries.APIQuery;
import com.datasqrl.plan.queries.APISubscription;
import com.datasqrl.schema.SQRLTable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.Value;
import org.apache.commons.lang3.tuple.Pair;

@Value
public class MockAPIConnectorManager implements APIConnectorManager {

  List<APIQuery> queries = new ArrayList<>();
  List<APIMutation> mutations = new ArrayList<>();

  @Override
  public void addMutation(APIMutation mutation) {
    mutations.add(mutation);
  }

  @Override
  public TableSource addSubscription(APISubscription subscription, SQRLTable sqrlTable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addQuery(APIQuery query) {
    queries.add(query);
  }

  @Override
  public ModuleLoader getAsModuleLoader() {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Log> getLogs() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<SQRLTable, Log> getExports() {
    throw new UnsupportedOperationException();
  }
}
