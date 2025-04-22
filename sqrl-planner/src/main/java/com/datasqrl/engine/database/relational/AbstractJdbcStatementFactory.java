package com.datasqrl.engine.database.relational;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.calcite.rel.RelNode;
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

import com.datasqrl.calcite.OperatorRuleTransformer;
import com.datasqrl.calcite.convert.RelToSqlNode;
import com.datasqrl.calcite.convert.SqlNodeToString;
import com.datasqrl.calcite.dialect.postgres.SqlCreatePostgresView;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.engine.database.relational.JdbcStatement.Field;
import com.datasqrl.engine.database.relational.JdbcStatement.Type;
import com.datasqrl.engine.database.relational.ddl.statements.CreateTableDDL;
import com.datasqrl.sql.DatabaseExtension;
import com.datasqrl.util.CalciteUtil;
import com.datasqrl.v2.dag.plan.MaterializationStagePlan.Query;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public abstract class AbstractJdbcStatementFactory implements JdbcStatementFactory {

  protected final OperatorRuleTransformer dialectCallConverter;
  protected final RelToSqlNode relToSqlConverter;
  protected final SqlNodeToString sqlNodeToString;

  @Override
  public QueryResult createQuery(Query query, boolean withView) {
    return createQuery(query.getFunction().getFullPath().getLast().getDisplay(), query.getRelNode(), withView);
  }

  public QueryResult createQuery(String viewName, RelNode relNode, boolean withView) {
    var rewrittenRelNode = dialectCallConverter.convert(relNode);
    var sqlNode = relToSqlConverter.convert(rewrittenRelNode);
    var sql = sqlNodeToString.convert(sqlNode).getSql();
    var qBuilder = ExecutableJdbcReadQuery.builder();
    qBuilder.sql(sql);

    JdbcStatement view = null;
    if (withView) {
      var viewNameIdentifier = new SqlIdentifier(viewName, SqlParserPos.ZERO);
      var columnList = new SqlNodeList(relNode.getRowType().getFieldList().stream()
          .map(f->new SqlIdentifier(f.getName(), SqlParserPos.ZERO))
          .collect(Collectors.toList()), SqlParserPos.ZERO);
      var viewSql = createView(viewNameIdentifier, columnList, sqlNode.getSqlNode());
      var datatype = relNode.getRowType();
      view = new JdbcStatement(viewName, Type.VIEW, viewSql, datatype, getColumns(datatype.getFieldList()));
    }
    return new JdbcStatementFactory.QueryResult(qBuilder, view);
  }

  @Override
  public JdbcStatement createTable(JdbcEngineCreateTable createTable) {
    var tableName = createTable.getTable().getTableName();
    var ddl = createTable(tableName, createTable.getDatatype().getFieldList(),
        createTable.getTable().getPrimaryKey().get());
    return new JdbcStatement(tableName, Type.TABLE, ddl.getSql(), createTable.getDatatype(),
        ddl.getColumns());
  }

  public CreateTableDDL createTable(String name, List<RelDataTypeField> fields, List<String> primaryKeys) {
    //TODO: Move to SqlNode
    var tableName = quoteIdentifier(name);
    var columns = getColumns(fields);
    var pks = quoteValues(primaryKeys);
    return new CreateTableDDL(tableName, columns, pks);
  }

  protected List<Field> getColumns(List<RelDataTypeField> fields) {
    return fields.stream()
        .map(this::toField)
        .collect(Collectors.toList());
  }

  protected JdbcStatement.Field toField(RelDataTypeField field) {
    var castSpec = getSqlType(field.getType());
    var sqlPrettyWriter = new SqlPrettyWriter();
    castSpec.unparse(sqlPrettyWriter, 0, 0);
    var typeName = sqlPrettyWriter.toSqlString().getSql();

    var datatype = field.getType();

    return new Field(field.getName(), typeName, datatype.isNullable());
  }

  protected abstract SqlDataTypeSpec getSqlType(RelDataType type);

  protected String createView(SqlIdentifier viewNameIdentifier,
      SqlNodeList columnList, SqlNode viewSqlNode) {
    var createView = new SqlCreatePostgresView(SqlParserPos.ZERO, true,
        viewNameIdentifier, columnList,
        viewSqlNode);
    return sqlNodeToString.convert(() -> createView).getSql();
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
            ((RawRelDataType) field.getType()).getRawType().getOriginatingClass().equals(extension.typeClass())) {
			matchedExtensions.add(extension);
		}
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
