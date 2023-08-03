package com.datasqrl.plan.local.generate;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.global.PhysicalDAGPlan.Query;
import com.datasqrl.plan.rules.LPAnalysis;
import com.datasqrl.plan.table.CalciteTableFactory;
import com.datasqrl.plan.table.QueryRelationalTable;
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
import org.apache.calcite.util.Litmus;

public class QueryStatementResolver extends AbstractQueryStatementResolver {

  protected QueryStatementResolver(ErrorCollector errors,
      NameCanonicalizer nameCanonicalizer, SqrlQueryPlanner planner, CalciteTableFactory tableFactory) {
    super(errors, nameCanonicalizer, planner, tableFactory);
  }

  @Override
  protected void createTable(Namespace ns, Assignment statement, LPAnalysis analyzedLP) {
    if (statement.getTableArgs().isPresent() && statement instanceof QueryAssignment) {
      QueryAssignment queryAssignment = (QueryAssignment) statement;
      ns.addNsObject(createTableFunction(ns, queryAssignment, analyzedLP));
    } else {
      super.createTable(ns, statement, analyzedLP);
    }
  }

  private TableFunctionNamespaceObject createTableFunction(Namespace ns, QueryAssignment statement, LPAnalysis analyzedLP) {
    Name functionName = statement.getNamePath().getLast();
    List<FunctionParameter> params = createTableParameters(statement.getTableArgs().get());
    Optional<SQRLTable> accessTable = getAccessTable(ns, statement);
    TableFunctionBase tableFunction;
    if (accessTable.isPresent()) {
      SQRLTable table = accessTable.get();
      tableFunction = new AccessTableFunction(functionName, params, analyzedLP.getOriginalRelnode(), table);
    } else {
      SqrlTableNamespaceObject t = super.createTableInternal(ns, statement, analyzedLP);
      tableFunction = new ComputeTableFunction(functionName, params, analyzedLP.getOriginalRelnode(), t.getTable().getTable(),
          (QueryRelationalTable) t.getTable().getBaseTable());
    }
    return new TableFunctionNamespaceObject(functionName, tableFunction);
  }

  private Optional<SQRLTable> getAccessTable(Namespace ns, QueryAssignment queryAssignment) {
    SqlSelect select = (SqlSelect)queryAssignment.getQuery();
    SqlIdentifier tableName;
    if (SqlUtil.stripAs(select.getFrom()).getKind() == SqlKind.IDENTIFIER) {
      tableName = (SqlIdentifier) SqlUtil.stripAs(select.getFrom());
    } else {
      return Optional.empty();
    }

    //TODO: This looks like a common piece of code and should reuse an existing function for
    //locating a SQRLTable by name in SqrlSchema
    NamePath names = NamePath.of(tableName.names.toArray(String[]::new));
    Optional<SQRLTable> table = ns.getSchema().getRootTables()
        .stream().filter(t -> t.getName().equals(names.getFirst()))
        .findAny();
    table = table.flatMap(t->t.walkTable(names.popFirst()));

    Preconditions.checkState(table.isPresent(), "Could not find table: %s",
        queryAssignment.getNamePath() );

    SQRLTable sqrlTable = table.get();
    //TODO: This is broken because * gets expanded. Need to have a better test to check if it was *
    //checking based on equal number of columns is not correct because they may be renamed or different.
    //if (!select.getSelectList().equalsDeep(SqlNodeList.SINGLETON_STAR, Litmus.IGNORE)) return Optional.empty();
    if (select.getSelectList().size()!=sqrlTable.getVisibleColumns().size()) return Optional.empty();
    return Optional.of(sqrlTable);
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
