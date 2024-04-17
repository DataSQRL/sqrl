package com.datasqrl.graphql;

import com.datasqrl.calcite.function.SqrlTableMacro;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.engine.log.Log;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.plan.queries.APIMutation;
import com.datasqrl.plan.queries.APIQuery;
import com.datasqrl.plan.queries.APISource;
import com.datasqrl.plan.queries.APISubscription;
import java.util.List;
import java.util.Map;

public interface APIConnectorManager extends APIConnectorLookup {

  /**
   * Adds mutation by connecting it to a table source and sink.
   * Those are either loaded if the module for the api source exists or created by the log engine.
   *
   * @param mutation
   */
  void addMutation(APIMutation mutation);

  TableSource getMutationSource(APISource source, Name mutationName);

  Log addSubscription(APISubscription subscription, SqrlTableMacro sqrlTable);

  void addQuery(APIQuery query);

  ModuleLoader getModuleLoader();

  List<APIQuery> getQueries();

  Map<SqrlTableMacro, Log> getExports();
}
