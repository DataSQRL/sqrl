package com.datasqrl.plan.global;


import static com.datasqrl.plan.validate.ScriptPlanner.exportTable;

import com.datasqrl.calcite.ModifiableTable;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.plan.local.generate.QueryTableFunction;
import com.datasqrl.plan.local.generate.ResolvedExport;
import com.datasqrl.plan.table.ProxyImportRelationalTable;
import com.datasqrl.plan.table.PhysicalRelationalTable;
import com.datasqrl.schema.RootSqrlTable;
import com.datasqrl.util.StreamUtil;
import com.google.common.base.Preconditions;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Value;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.calcite.tools.RelBuilder;

@Value
public class DAGPreparation {

  private final RelBuilder relBuilder;

  private final ErrorCollector errors;

  public Collection<AnalyzedAPIQuery> prepareInputs(SqrlSchema sqrlSchema, APIConnectorManager apiManager,
      Collection<ResolvedExport> exports) {

    //Add subscriptions as exports
    apiManager.getExports().forEach((sqrlTable, log) -> exports.add(exportTable(
        (ModifiableTable) ((RootSqrlTable) sqrlTable).getInternalTable(),
        log.getSink(), relBuilder)));

    //Assign timestamps to imports which propagate and restrict remaining timestamps in downstream tables
    StreamUtil.filterByClass(getAllPhysicalTables(sqrlSchema), ProxyImportRelationalTable.class)
        .forEach(this::finalizeTimestampOnTable);
    //Some downstream tables have multiple timestamp candidates because the timestamp was selected multiple times, pick the best one
    getAllPhysicalTables(sqrlSchema).forEach(this::finalizeTimestampOnTable);

    //Replace default joins with inner joins for API queries
    return apiManager.getQueries().stream().map(AnalyzedAPIQuery::new
    ).collect(Collectors.toList());
  }

  private Stream<PhysicalRelationalTable> getAllPhysicalTables(SqrlSchema sqrlSchema) {
    return Stream.concat(sqrlSchema.getTableStream(PhysicalRelationalTable.class),
            sqrlSchema.getFunctionStream(QueryTableFunction.class).map(
                    QueryTableFunction::getQueryTable));
  }

  private void finalizeTimestampOnTable(PhysicalRelationalTable table) {
    // Determine best timestamp if not already fixed
    if (table.getType().hasTimestamp() && !table.getTimestamp().hasFixedTimestamp()) {
      table.getTimestamp().selectTimestamp();
    }
    Preconditions.checkArgument(!table.getType().hasTimestamp() || table.getTimestamp().hasFixedTimestamp());
  }
}
