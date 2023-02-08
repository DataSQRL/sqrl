package com.datasqrl.plan.local.generate;

import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.loaders.DataSystemNsObject;
import com.datasqrl.name.NamePath;
import com.datasqrl.parse.SqrlAstException;
import com.datasqrl.plan.local.generate.SqrlStatementVisitor.SystemContext;
import com.datasqrl.schema.SQRLTable;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.ExportDefinition;
import org.apache.calcite.tools.RelBuilder;

public class ExportStatementResolver extends AbstractStatementResolver {

  public ExportStatementResolver(SystemContext systemContext) {
    super(systemContext);
  }

  public void resolve(ExportDefinition statement, Namespace ns) {
    Optional<SQRLTable> tableOpt = resolveTable(ns, statement.getNamePath(),
        false);
    checkState(tableOpt.isPresent(), ErrorCode.MISSING_DEST_TABLE,
        statement.getTablePath()::getParserPosition,
        () -> String.format("Could not find table path: %s", statement.getTablePath()));
    SQRLTable table = tableOpt.get();
    checkState(table.getVt().getRoot().getBase().getExecution().isWrite(),
        ErrorCode.READ_TABLE_CANNOT_BE_EXPORTED, statement.getTablePath()::getParserPosition,
        () -> String.format("Table [%s] is not be exported because it is not computed in-stream",
            table.getPath()));
    NamePath sinkPath = toNamePath(statement.getSinkPath());
    exportTable(table, sinkPath, ns, statement,
        systemContext.getErrors()
            .atFile(SqrlAstException.toLocation(statement.getSinkPath().getParserPosition())));
  }

  public void exportTable(SQRLTable table, NamePath sinkPath, Namespace ns,
      ExportDefinition statement, ErrorCollector errors) {
    Preconditions.checkArgument(table.getVt().getRoot().getBase().getExecution().isWrite());

    Optional<TableSink> sink = systemContext.getModuleLoader()
        .getModule(sinkPath.popLast())
        .flatMap(m->m.getNamespaceObject(sinkPath.popLast().getLast()))
        .map(s -> ((DataSystemNsObject) s).getTable())
        .flatMap(dataSystem -> dataSystem.discoverSink(sinkPath.getLast(), errors))
        .map(tblConfig ->
            tblConfig.initializeSink(errors, sinkPath, Optional.empty()));

    errors.checkFatal(sink.isPresent(), ErrorCode.CANNOT_RESOLVE_TABLESINK,
        "Cannot resolve table sink: %s", sinkPath);
    RelBuilder relBuilder = ns.session.createRelBuilder()
        .scan(table.getVt().getNameId());
    List<RexNode> selects = new ArrayList<>();
    List<String> fieldNames = new ArrayList<>();
    table.getVisibleColumns().stream().forEach(c -> {
      selects.add(relBuilder.field(c.getShadowedName().getCanonical()));
      fieldNames.add(c.getName().getDisplay());
    });
    relBuilder.project(selects, fieldNames);
    ns.addExport(new ResolvedExport(table.getVt(), relBuilder.build(), sink.get()));

  }
}
