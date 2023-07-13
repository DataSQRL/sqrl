package com.datasqrl.plan.local.generate;

import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.rules.LPAnalysis;
import com.datasqrl.plan.table.CalciteTableFactory;
import com.datasqrl.schema.SQRLTable;
import com.google.common.base.Preconditions;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class QueryStatementResolver extends AbstractQueryStatementResolver {

  protected QueryStatementResolver(ErrorCollector errors,
      NameCanonicalizer nameCanonicalizer, SqrlQueryPlanner planner, CalciteTableFactory tableFactory) {
    super(errors, nameCanonicalizer, planner, tableFactory);
  }

  @Override
  protected SqrlTableNamespaceObject createTable(Namespace ns, Assignment statement, LPAnalysis analyzedLP) {
    if (statement.getTableArgs().isPresent() && statement instanceof QueryAssignment) {
      QueryAssignment queryAssignment = (QueryAssignment) statement;

      ns.getSchema().plus()
          .add(statement.getNamePath().get(0).getDisplay(),
              createTableFunction(ns, queryAssignment, analyzedLP));
    } else {
      super.createTable(ns, statement, analyzedLP);
    }
    return null;
  }

  private TableFunctionBase createTableFunction(Namespace ns, QueryAssignment statement, LPAnalysis analyzedLP) {
    List<FunctionParameter> params = createTableParameters(statement.getTableArgs().get());

    if (isAccessTableFunction(ns, statement, analyzedLP)) {
      SQRLTable table = getSqrlTable(ns, statement);
      return new AccessTableFunction(analyzedLP, params, table.getVt().getRoot().getBase());
    } else {
      SqrlTableNamespaceObject t = super.createTable(ns, statement, analyzedLP);
      return new ComputeTableFunction(analyzedLP, params, t.getTable().getBaseTable());
    }
  }

  private SQRLTable getSqrlTable(Namespace ns, QueryAssignment queryAssignment) {
    SqlSelect select = (SqlSelect)queryAssignment.getQuery();
    SqlIdentifier tableName;
    if (select.getFrom().getKind() == SqlKind.AS) {
      tableName = (SqlIdentifier)((SqlBasicCall)select.getFrom()).getOperandList().get(0);
    } else {
      tableName = (SqlIdentifier) select.getFrom();
    }

    NamePath names = NamePath.of(tableName.names.toArray(String[]::new));
    Optional<SQRLTable> table = ns.getSchema().getRootTables()
        .stream().filter(t -> t.getName().equals(names.getFirst()))
        .findAny();
    table = table.flatMap(t->t.walkTable(names.popFirst()));

    Preconditions.checkState(table.isPresent(), "Could not find table: %s",
        queryAssignment.getNamePath() );

    return table.get();
  }

  private boolean isAccessTableFunction(Namespace ns, QueryAssignment queryAssignment, LPAnalysis analyzedLP) {
    SQRLTable table = getSqrlTable(ns, queryAssignment);
    return analyzedLP.getOriginalRelnode().getRowType().equalsSansFieldNames(table.getVt().getRowType());
  }

  private List<FunctionParameter> createTableParameters(List<TableFunctionArgument> tableFunctionArguments) {
    AtomicInteger i = new AtomicInteger();
    return tableFunctionArguments.stream()
        .map(f->new FunctionParameter() {

          @Override
          public int getOrdinal() {
            return i.getAndIncrement();
          }

          @Override
          public String getName() {
            return f.getName().getSimple();
          }

          @Override
          public RelDataType getType(RelDataTypeFactory relDataTypeFactory) {
            return relDataTypeFactory.createSqlType(
                SqlTypeName.valueOf(f.getType().getTypeName().getSimple()));
          }

          @Override
          public boolean isOptional() {
            return false;
          }
        })
        .collect(Collectors.toList());
  }
}
