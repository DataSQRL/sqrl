package com.datasqrl.plan.local.generate;

import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorLabel;
import com.datasqrl.name.NameCanonicalizer;
import com.datasqrl.name.NamePath;
import com.datasqrl.name.ReservedName;
import com.datasqrl.parse.SqrlAstException;
import com.datasqrl.plan.calcite.rules.AnnotatedLP;
import com.datasqrl.plan.local.generate.SqrlStatementVisitor.SystemContext;
import com.datasqrl.schema.SQRLTable;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqrlStatement;
import org.apache.calcite.sql.parser.SqlParserPos;

public abstract class AbstractStatementResolver {

  protected final SystemContext systemContext;

  public AbstractStatementResolver(SystemContext systemContext) {
    this.systemContext = systemContext;
  }

  protected SqlNode transpile(SqrlStatement sqlNode, Namespace ns) {
    Transpiler transpiler = new Transpiler(systemContext);
    return transpiler.transpile(sqlNode, ns);
  }

  protected void addColumn(NamePath namePath, Namespace ns, RelNode relNode, boolean lockTimestamp) {
    SQRLTable table = getContext(ns, namePath)
        .orElseThrow(()->new RuntimeException("Could not find table"));

    table.addColumn(namePath.getLast(), relNode, lockTimestamp, ns.session.createRelBuilder(), ns.tableFactory);
  }

  protected Optional<SQRLTable> getContext(Namespace ns, NamePath namePath) {
    return resolveTable(ns, namePath, true);
  }

  protected Optional<SQRLTable> resolveTable(Namespace ns, NamePath namePath, boolean getParent) {
    if (getParent && !namePath.isEmpty()) {
      namePath = namePath.popLast();
    }
    if (namePath.isEmpty()) {
      return Optional.empty();
    }
    Optional<SQRLTable> table =
        Optional.ofNullable(ns.getSchema().getTable(namePath.get(0).getDisplay(), false))
            .map(t -> (SQRLTable) t.getTable());
    NamePath childPath = namePath.popFirst();
    return table.flatMap(t -> t.walkTable(childPath));
  }

  protected RelNode plan(SqlNode sqlNode, Namespace ns) {
    Planner planner = new Planner(systemContext);
    return planner.plan(sqlNode, ns);
  }
  protected AnnotatedLP convert(RelNode relNode, Namespace ns,
      Function<AnnotatedLP, AnnotatedLP> postProcess, boolean isStream, Optional<SqlNodeList> hints) {
    Converter planner = new Converter(systemContext);
    AnnotatedLP annotatedLP = planner.convert(relNode, ns, isStream, hints);
    return postProcess.apply(annotatedLP);
  }

  //  private void exportTable(SQRLTable table, NamePath sinkPath, Env env, ErrorCollector errors) {
//    Preconditions.checkArgument(table.getVt().getRoot().getBase().getExecution().isWrite());
//    Optional<TableSink> sink = env.getExporter().export(new LoaderContextImpl(env, env.namespace, new ResourceResolver(basePath), sinkPath), sinkPath);
//    errors.checkFatal(sink.isPresent(), ErrorCode.CANNOT_RESOLVE_TABLESINK,
//        "Cannot resolve table sink: %s", sinkPath);
//    RelBuilder relBuilder = env.createRelBuilder()
//        .scan(table.getVt().getNameId());
//    List<RexNode> selects = new ArrayList<>();
//    List<String> fieldNames = new ArrayList<>();
//    table.getVisibleColumns().stream().forEach(c -> {
//      selects.add(relBuilder.field(c.getShadowedName().getCanonical()));
//      fieldNames.add(c.getName().getDisplay());
//    });
//    relBuilder.project(selects, fieldNames);
//    env.exports.add(new ResolvedExport(table.getVt(), relBuilder.build(), sink.get()));
//  }
//

  public NamePath toNamePath(SqlIdentifier identifier) {
    return toNamePath(systemContext.getNameCanonicalizer(), identifier);
  }

  public static NamePath toNamePath(NameCanonicalizer nameCanonicalizer, SqlIdentifier identifier) {
    return NamePath.of(identifier.names.stream()
        .map(i -> (i.equals(""))
            ? ReservedName.ALL
            : nameCanonicalizer.name(i)
        )
        .collect(Collectors.toList()));
  }

  public void checkState(boolean check, ErrorLabel label, SqlNode node) {
    checkState(check, label, node::getParserPosition, () -> "");
  }

  public void checkState(boolean check, ErrorLabel label, SqlNode node, String message) {
    checkState(check, label, node::getParserPosition, () -> message);
  }

  public static void checkState(boolean check, ErrorLabel label,
      Supplier<SqlParserPos> pos, Supplier<String> message) {
    if (!check) {
      throw createAstException(label, pos, message);
    }
  }

  public static RuntimeException createAstException(ErrorLabel label, Supplier<SqlParserPos> pos,
      Supplier<String> message) {
    return new SqrlAstException(label, pos.get(), message.get());
  }

  private RuntimeException fatal(SqlParserPos pos, String message) {
    return createAstException(ErrorCode.GENERIC,
        () -> pos, () -> message);
  }

  private RuntimeException unsupportedOperation(SqlParserPos pos, String message) {
    return createAstException(ErrorCode.GENERIC,
        () -> pos, () -> message);
  }

}
