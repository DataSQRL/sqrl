package com.datasqrl.engine.database.relational;

import com.datasqrl.calcite.convert.RelToSqlNode;
import com.datasqrl.calcite.convert.RelToSqlNode.SqlNodes;
import com.datasqrl.calcite.convert.SqlNodeToString;
import com.datasqrl.calcite.dialect.postgres.SqlCreatePostgresView;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.engine.database.relational.JdbcStatement.Field;
import com.datasqrl.engine.database.relational.JdbcStatement.Type;
import com.datasqrl.engine.database.relational.ddl.statements.CreateTableDDL;
import com.datasqrl.engine.stream.flink.sql.RelToFlinkSql;
import com.datasqrl.sql.DatabaseExtension;
import com.datasqrl.util.CalciteUtil;
import com.datasqrl.v2.dag.plan.MaterializationStagePlan.Query;
import com.datasqrl.v2.tables.SqrlTableFunction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.flink.table.planner.plan.schema.RawRelDataType;

@AllArgsConstructor
public abstract class AbstractJdbcStatementFactory implements JdbcStatementFactory {

  protected final RelToSqlNode relToSqlConverter;
  protected final SqlNodeToString sqlNodeToString;

  @Override
  public QueryResult createQuery(Query query, boolean withView) {
    return createQuery(query.getFunction().getFunctionCatalogName(), query.getRelNode(), withView);
  }

  public QueryResult createQuery(String viewName, RelNode relNode, boolean withView) {
    SqlNodes sqlNode = relToSqlConverter.convert(relNode);
    String sql = sqlNodeToString.convert(sqlNode).getSql();
    ExecutableJdbcReadQuery.ExecutableJdbcReadQueryBuilder qBuilder = ExecutableJdbcReadQuery.builder();
    qBuilder.sql(sql);

    JdbcStatement view = null;
    if (withView) {
      SqlIdentifier viewNameIdentifier = new SqlIdentifier(viewName, SqlParserPos.ZERO);
      SqlNodeList columnList = new SqlNodeList(relNode.getRowType().getFieldList().stream()
          .map(f->new SqlIdentifier(f.getName(), SqlParserPos.ZERO))
          .collect(Collectors.toList()), SqlParserPos.ZERO);
      String viewSql = createView(viewNameIdentifier, columnList, sqlNode.getSqlNode());
      RelDataType datatype = relNode.getRowType();
      view = new JdbcStatement(viewName, Type.VIEW, viewSql, datatype, getColumns(datatype.getFieldList()));
    }
    return new JdbcStatementFactory.QueryResult(qBuilder, view);
  }

  @Override
  public JdbcStatement createTable(JdbcEngineCreateTable createTable) {
    String tableName = createTable.getTable().getTableName();
    CreateTableDDL ddl = createTable(tableName, createTable.getDatatype().getFieldList(),
        createTable.getTable().getPrimaryKey().get());
    return new JdbcStatement(tableName, Type.TABLE, ddl.getSql(), createTable.getDatatype(),
        ddl.getColumns());
  }

  public CreateTableDDL createTable(String name, List<RelDataTypeField> fields, List<String> primaryKeys) {
    //TODO: Move to SqlNode
    String tableName = quoteIdentifier(name);
    List<Field> columns = getColumns(fields);
    List<String> pks = quoteValues(primaryKeys);
    return new CreateTableDDL(tableName, columns, pks);
  }

  protected List<Field> getColumns(List<RelDataTypeField> fields) {
    return fields.stream()
        .map(this::toField)
        .collect(Collectors.toList());
  }

  protected JdbcStatement.Field toField(RelDataTypeField field) {
    SqlDataTypeSpec castSpec = getSqlType(field.getType());
    SqlPrettyWriter sqlPrettyWriter = new SqlPrettyWriter();
    castSpec.unparse(sqlPrettyWriter, 0, 0);
    String typeName = sqlPrettyWriter.toSqlString().getSql();

    RelDataType datatype = field.getType();

    return new Field(field.getName(), typeName, datatype.isNullable());
  }

  protected abstract SqlDataTypeSpec getSqlType(RelDataType type);

  protected String createView(SqlIdentifier viewNameIdentifier,
      SqlNodeList columnList, SqlNode viewSqlNode) {
    SqlCreatePostgresView createView = new SqlCreatePostgresView(SqlParserPos.ZERO, true,
        viewNameIdentifier, columnList,
        viewSqlNode);
    return sqlNodeToString.convert(() -> createView).getSql() + ";";
  }

  public static List<String> quoteIdentifier(List<String> columns) {
    return columns.stream()
        .map(AbstractJdbcStatementFactory::quoteIdentifier)
        .collect(Collectors.toList());
  }
  public static String quoteIdentifier(String column) {
    return "\"" + column + "\"";
  }

  public static List<String> quoteValues(List<String> values) {
    return values.stream()
        .map(AbstractJdbcStatementFactory::quoteIdentifier)
        .collect(Collectors.toList());
  }

  protected Set<DatabaseExtension> extractTypeExtensions(Stream<RelNode> relNodes, List<DatabaseExtension> extensions) {
    return relNodes.map(relNode -> extractTypeExtensions(relNode, extensions))
        .flatMap(Collection::stream).collect(Collectors.toSet());
  }

  protected Set<DatabaseExtension> extractTypeExtensions(RelNode relNode, List<DatabaseExtension> extensions) {
    Set<DatabaseExtension> matchedExtensions = new HashSet<>();
    for (RelDataTypeField field : relNode.getRowType().getFieldList()) {
      //See if we use a type from an extension by matching on class
      for (DatabaseExtension extension : extensions) {
        if (field.getType() instanceof RawRelDataType &&
            ((RawRelDataType) field.getType()).getRawType().getOriginatingClass().equals(extension.typeClass()))
          matchedExtensions.add(extension);
      }
    }

    //See if we match the extension by operator
    CalciteUtil.applyRexShuttleRecursively(relNode, new RexShuttle() {
      @Override
      public RexNode visitCall(RexCall call) {
        for (DatabaseExtension extension : extensions) {
          for (Name functionName : extension.operators()) {
            if (functionName.equals(
                Name.system(call.getOperator().getName()))) {
              matchedExtensions.add(extension);
            }
          }
        }

        return super.visitCall(call);
      }
    });

    return matchedExtensions;
  }

}
