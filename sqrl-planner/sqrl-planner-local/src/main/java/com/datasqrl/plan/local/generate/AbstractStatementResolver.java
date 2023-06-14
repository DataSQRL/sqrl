package com.datasqrl.plan.local.generate;

import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorLabel;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.canonicalizer.ReservedName;
import com.datasqrl.parse.SqrlAstException;
import com.datasqrl.schema.SQRLTable;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqrlStatement;
import org.apache.calcite.sql.parser.SqlParserPos;

public abstract class AbstractStatementResolver {

  ErrorCollector errors;
  private final NameCanonicalizer nameCanonicalizer;

  SqrlQueryPlanner planner;

  protected AbstractStatementResolver(ErrorCollector errors, NameCanonicalizer nameCanonicalizer,
      SqrlQueryPlanner planner) {
    this.errors = errors;
    this.nameCanonicalizer = nameCanonicalizer;
    this.planner = planner;
  }

  protected SqlNode transpile(SqrlStatement sqlNode, Namespace ns) {
    Transpiler transpiler = new Transpiler();
    return transpiler.transpile(sqlNode, ns, errors);
  }

  protected void addColumn(NamePath namePath, Namespace ns, RelNode relNode, boolean lockTimestamp) {
    SQRLTable table = getContext(ns, namePath)
        .orElseThrow(()->new RuntimeException("Could not find table"));

    table.addColumn(namePath.getLast(), relNode, lockTimestamp);
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

  //TODO: operator table is namespace dependent but not exposed
  // in the planner in a coherent way
  protected RelNode plan(SqlNode sqlNode) {
    return planner.plan(sqlNode);
  }

  public NamePath toNamePath(SqlIdentifier identifier) {
    return toNamePath(nameCanonicalizer, identifier);
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
