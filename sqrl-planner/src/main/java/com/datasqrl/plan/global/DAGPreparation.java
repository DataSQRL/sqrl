package com.datasqrl.plan.global;


import com.datasqrl.calcite.ModifiableTable;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.plan.local.generate.QueryTableFunction;
import com.datasqrl.plan.local.generate.ResolvedExport;
import com.datasqrl.plan.table.PhysicalRelationalTable;
import com.datasqrl.schema.RootSqrlTable;
import com.google.inject.Inject;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.calcite.tools.RelBuilder;

import static com.datasqrl.plan.validate.ScriptPlanner.exportTable;

@AllArgsConstructor(onConstructor_=@Inject)
public class DAGPreparation {

  private final RelBuilder relBuilder;
  private final APIConnectorManager apiManager;

  public Collection<AnalyzedAPIQuery> prepareInputs(SqrlSchema sqrlSchema,
      Collection<ResolvedExport> exports) {

    //Add subscriptions as exports
    apiManager.getExports().forEach((sqrlTable, log) -> exports.add(exportTable(
        (ModifiableTable) ((RootSqrlTable) sqrlTable).getInternalTable(),
        log.getSink(), relBuilder, false)));

    //Replace default joins with inner joins for API queries
    return apiManager.getQueries().stream()
        .map(AnalyzedAPIQuery::new).collect(Collectors.toList());
  }

  private Stream<PhysicalRelationalTable> getAllPhysicalTables(SqrlSchema sqrlSchema) {
    return Stream.concat(sqrlSchema.getTableStream(PhysicalRelationalTable.class),
            sqrlSchema.getFunctionStream(QueryTableFunction.class).map(
                    QueryTableFunction::getQueryTable));
  }

}
