package com.datasqrl.plan.local.generate;

import static com.datasqrl.error.PosToErrorPos.atPosition;

import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.loaders.DataSystemNsObject;
import com.datasqrl.loaders.LoaderUtil;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.plan.queries.APISubscription;
import com.datasqrl.schema.SQRLTable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.ExportDefinition;
import org.apache.calcite.tools.RelBuilder;

public class ExportStatementResolver extends AbstractStatementResolver {

  private final ModuleLoader moduleLoader;

  protected ExportStatementResolver(ModuleLoader moduleLoader, ErrorCollector errors,
      NameCanonicalizer nameCanonicalizer, SqrlQueryPlanner planner) {
    super(errors, nameCanonicalizer, planner);
    this.moduleLoader = moduleLoader;
  }

  public void resolve(ExportDefinition statement, Namespace ns) {
    Optional<SQRLTable> tableOpt = resolveTable(ns, statement.getNamePath(),
        false);
    checkState(tableOpt.isPresent(), ErrorCode.MISSING_DEST_TABLE,
        statement.getTablePath()::getParserPosition,
        () -> String.format("Could not find table path: %s", statement.getTablePath()));
    SQRLTable table = tableOpt.get();
    NamePath sinkPath = toNamePath(statement.getSinkPath());
    exportTable(table, sinkPath, ns, statement, errors);
  }

  private void exportTable(SQRLTable table, NamePath sinkPath, Namespace ns,
      ExportDefinition statement, ErrorCollector errors) {
    ErrorCollector sinkErrors = errors.withLocation(atPosition(errors,
        statement.getSinkPath().getParserPosition()));
    TableSink sink = LoaderUtil.loadSink(sinkPath, sinkErrors, moduleLoader);
    ns.addExport(exportTable(table,sink,planner.createRelBuilder()));
  }

  public static ResolvedExport exportTable(SQRLTable table, TableSink sink, RelBuilder relBuilder) {
    relBuilder.scan(table.getVt().getNameId());
    List<RexNode> selects = new ArrayList<>();
    List<String> fieldNames = new ArrayList<>();
    table.getVisibleColumns().stream().forEach(c -> {
      selects.add(relBuilder.field(c.getShadowedName().getCanonical()));
      fieldNames.add(c.getName().getDisplay());
    });
    relBuilder.project(selects, fieldNames);
    return new ResolvedExport(table.getVt(), relBuilder.build(), sink);
  }
}
