package com.datasqrl.plan.global;


import com.datasqrl.calcite.ModifiableTable;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.APIConnectorLookup;
import com.datasqrl.plan.local.generate.QueryTableFunction;
import com.datasqrl.plan.local.generate.ResolvedExport;
import com.datasqrl.plan.table.PhysicalRelationalTable;
import lombok.Value;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.calcite.tools.RelBuilder;

import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.datasqrl.plan.validate.ScriptPlanner.exportTable;

@Value
public class DAGPreparation {

  private final RelBuilder relBuilder;

  private final ErrorCollector errors;

  public Collection<AnalyzedAPIQuery> prepareInputs(SqrlSchema sqrlSchema, APIConnectorLookup apiManager,
      Collection<ResolvedExport> exports) {

    //Add subscriptions as exports
    apiManager.getExports().forEach((sqrlTable, log) -> exports.add(exportTable((ModifiableTable) sqrlTable.getVt(),
        log.getSink(), relBuilder, false)));

    //Replace default joins with inner joins for API queries
    return apiManager.getQueries().stream().map(apiQuery ->
      new AnalyzedAPIQuery(apiQuery.getNameId(), apiQuery.getRelNode())
    ).collect(Collectors.toList());
  }

  private Stream<PhysicalRelationalTable> getAllPhysicalTables(SqrlSchema sqrlSchema) {
    return Stream.concat(sqrlSchema.getTableStream(PhysicalRelationalTable.class),
            sqrlSchema.getFunctionStream(QueryTableFunction.class).map(
                    QueryTableFunction::getQueryTable));
  }

}
