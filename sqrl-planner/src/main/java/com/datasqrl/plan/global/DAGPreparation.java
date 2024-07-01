package com.datasqrl.plan.global;


import com.datasqrl.calcite.ModifiableTable;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.ConnectorFactoryContext;
import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.LogEngineSupplier;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.SystemBuiltInConnectors;
import com.datasqrl.engine.log.Log;
import com.datasqrl.engine.log.LogEngine.Timestamp;
import com.datasqrl.engine.log.LogEngine.TimestampType;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.io.tables.TableSinkImpl;
import com.datasqrl.plan.local.generate.QueryTableFunction;
import com.datasqrl.plan.local.generate.ResolvedExport;
import com.datasqrl.plan.table.PhysicalRelationalTable;
import com.datasqrl.schema.RootSqrlTable;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.RelBuilder;

@AllArgsConstructor(onConstructor_=@Inject)
public class DAGPreparation {

  private final RelBuilder relBuilder;
  private final APIConnectorManager apiManager;
  private final LogEngineSupplier logEngineSupplier;
  private final ConnectorFactoryFactory connectorFactoryFactory;
  PackageJson packageJson;


  public Result prepareInputs(SqrlSchema sqrlSchema,
      Collection<ResolvedExport> exports) {

    List<Log> logs = new ArrayList<>();
    //Analyze exports
    List<AnalyzedExport> analyzedExports = new ArrayList<>();
    for (ResolvedExport export : exports) {
      if (export instanceof ResolvedExport.External) {
        ResolvedExport.External externalExport = (ResolvedExport.External) export;
        analyzedExports.add(AnalyzedExport.from(externalExport));
      } else {
        ResolvedExport.Internal internalExport = (ResolvedExport.Internal) export;
        SystemBuiltInConnectors connector = internalExport.getConnector();
        TableSink tableSink = null;
        if (connector == SystemBuiltInConnectors.LOG) {
          switch (packageJson.getLogMethod()) {
            case NONE: continue; //Ignore export
            case PRINT: connector = SystemBuiltInConnectors.PRINT_SINK; break;
            case LOG_ENGINE:
              Log sinkLog = logEngineSupplier.get().getLogFactory().create(export.getTable(), export.getTable(),
                  export.getRelNode().getRowType(), List.of(), Timestamp.NONE);
              logs.add(sinkLog);
              tableSink = sinkLog.getSink();
              break;
            default: throw new UnsupportedOperationException("Unknown log method: " + packageJson.getLogMethod());
          }
        }
        if (tableSink == null) {
          //Create the export for the built-in connector
          NamePath sinkPath = NamePath.of(connector.getName(), Name.system(export.getTable()));
          tableSink = connectorFactoryFactory.create(connector)
              .map(t -> t.createSourceAndSink(new ConnectorFactoryContext(sinkPath.getLast(),
                  Map.of("name", sinkPath.getLast().getDisplay()))))
              .map(t -> TableSinkImpl.create(t, sinkPath, Optional.empty())).get();
        }
        analyzedExports.add(new AnalyzedExport(export.getTable(), export.getRelNode(),
            OptionalInt.of(export.getNumFieldSelects()), tableSink));
      }
    }

    //Add all mutation and subscription logs
    logs.addAll(apiManager.getLogs());

    //Add subscriptions as exports
    apiManager.getExports().forEach((sqrlTable, log) -> {
      ModifiableTable modTable = (ModifiableTable) ((RootSqrlTable) sqrlTable).getInternalTable();
      RelNode relNode = relBuilder.scan(modTable.getNameId()).build();
      analyzedExports.add(new AnalyzedExport(modTable.getNameId(), relNode, OptionalInt.empty(), log.getSink()));
    });

    //Replace default joins with inner joins for API queries
    return new Result(apiManager.getQueries().stream()
        .map(AnalyzedAPIQuery::new).collect(Collectors.toList()),
        analyzedExports, logs);
  }

  private Stream<PhysicalRelationalTable> getAllPhysicalTables(SqrlSchema sqrlSchema) {
    return Stream.concat(sqrlSchema.getTableStream(PhysicalRelationalTable.class),
            sqrlSchema.getFunctionStream(QueryTableFunction.class).map(
                    QueryTableFunction::getQueryTable));
  }

  @Value
  public static class Result {

    Collection<AnalyzedAPIQuery> queries;
    Collection<AnalyzedExport> exports;
    List<Log> logs;


  }

}
