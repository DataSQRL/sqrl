package com.datasqrl.plan.global;

import com.datasqrl.calcite.ModifiableTable;
import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.PackageJson;
import com.datasqrl.engine.log.LogManager;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.plan.local.generate.QueryTableFunction;
import com.datasqrl.plan.local.generate.ResolvedExport;
import com.datasqrl.plan.table.PhysicalRelationalTable;
import com.datasqrl.schema.RootSqrlTable;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.OptionalInt;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.RelBuilder;

@AllArgsConstructor(onConstructor_ = @Inject)
public class DAGPreparation {

  private final RelBuilder relBuilder;
  private final APIConnectorManager apiManager;
  private final LogManager logManager;
  private final ConnectorFactoryFactory connectorFactoryFactory;
  PackageJson packageJson;

  public Result prepareInputs(SqrlSchema sqrlSchema, Collection<ResolvedExport> exports) {

    // Analyze exports
    List<AnalyzedExport> analyzedExports = new ArrayList<>();
    exports.stream().map(AnalyzedExport::from).forEach(analyzedExports::add);

    // Add subscriptions as exports
    apiManager
        .getExports()
        .forEach(
            (sqrlTable, log) -> {
              ModifiableTable modTable =
                  (ModifiableTable) ((RootSqrlTable) sqrlTable).getInternalTable();
              RelNode relNode = relBuilder.scan(modTable.getNameId()).build();
              analyzedExports.add(
                  new AnalyzedExport(
                      modTable.getNameId(), relNode, OptionalInt.empty(), log.getSink()));
            });

    // Replace default joins with inner joins for API queries
    return new Result(
        apiManager.getQueries().stream().map(AnalyzedAPIQuery::new).collect(Collectors.toList()),
        analyzedExports);
  }

  private Stream<PhysicalRelationalTable> getAllPhysicalTables(SqrlSchema sqrlSchema) {
    return Stream.concat(
        sqrlSchema.getTableStream(PhysicalRelationalTable.class),
        sqrlSchema
            .getFunctionStream(QueryTableFunction.class)
            .map(QueryTableFunction::getQueryTable));
  }

  @Value
  public static class Result {

    Collection<AnalyzedAPIQuery> queries;
    Collection<AnalyzedExport> exports;
  }
}
