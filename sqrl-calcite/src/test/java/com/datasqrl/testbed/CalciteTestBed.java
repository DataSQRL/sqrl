package com.datasqrl.testbed;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import org.apache.calcite.config.Lex;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlDialect.DatabaseProduct;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.testng.annotations.Test;

/** A simple calcite test bed */
public class CalciteTestBed {

  public class CustomSchema extends AbstractSchema {
    @Override
    protected Map<String, Table> getTableMap() {
      Map<String, Table> tableMap = new HashMap<>();
      tableMap.put("customers", new CustomTable());
      tableMap.put("orders", new CustomTable());
      return tableMap;
    }
  }

  public class CustomTable extends AbstractTable {
    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {

      RelDataTypeFactory.Builder builder = typeFactory.builder();
      builder.add("id", typeFactory.createSqlType(SqlTypeName.INTEGER));
      builder.add("name", typeFactory.createSqlType(SqlTypeName.VARCHAR));

      RelDataType entries =
          typeFactory.createStructType(
              List.of(typeFactory.createSqlType(SqlTypeName.VARCHAR)), List.of("col"));
      RelDataType arrayType = typeFactory.createArrayType(entries, -1);
      builder.add("entries", arrayType);
      return builder.build();
    }
  }

  @SneakyThrows
  @Test
  public void test() {
    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    rootSchema.add("CUSTOM_SCHEMA", new CustomSchema());

    FrameworkConfig config =
        Frameworks.newConfigBuilder()
            .defaultSchema(rootSchema.getSubSchema("CUSTOM_SCHEMA"))
            .parserConfig(SqlParser.config().withLex(Lex.JAVA))
            .build();

    Planner planner = Frameworks.getPlanner(config);

    try {
      SqlNode sqlNode = planner.parse("SELECT *\n" + "FROM orders o CROSS JOIN UNNEST(o.entries)");
      SqlNode validate = planner.validate(sqlNode);
      RelRoot relRoot = planner.rel(validate);

      // Unparse
      RelToSqlConverter converter = new RelToSqlConverter(DatabaseProduct.CALCITE.getDialect());
      final SqlNode sqlNodeUnparsed = converter.visitRoot(relRoot.rel).asStatement();
      String sql =
          sqlNodeUnparsed
              .toSqlString(
                  c ->
                      c.withAlwaysUseParentheses(false)
                          .withSelectListItemsOnSeparateLines(false)
                          .withUpdateSetListNewline(false)
                          .withIndentation(0)
                          .withDialect(DatabaseProduct.CALCITE.getDialect()))
              .getSql();

      Preconditions.checkState(!sql.isEmpty());

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
