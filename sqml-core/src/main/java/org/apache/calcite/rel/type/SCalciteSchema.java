package org.apache.calcite.rel.type;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import lombok.AllArgsConstructor;
import org.apache.calcite.jdbc.SqrlToCalciteTableTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.type.SqlTypeName;

@AllArgsConstructor
public class SCalciteSchema implements Schema {
    SqrlToCalciteTableTranslator tableTranslator;

  @Override
  public Table getTable(String table) {
    return tableTranslator.get(table);
  }

  @Override
  public Set<String> getTableNames() {
    return Set.of();
  }

  @Override
  public RelProtoDataType getType(String s) {
    return null;
  }

  @Override
  public Set<String> getTypeNames() {
    return Set.of();
  }

  @Override
  public Collection<Function> getFunctions(String s) {
    return List.of(new ScalarFunction() {
      @Override
      public RelDataType getReturnType(RelDataTypeFactory relDataTypeFactory) {
        return relDataTypeFactory.createSqlType(SqlTypeName.TIMESTAMP);
      }

      @Override
      public List<FunctionParameter> getParameters() {
        return List.of();
      }
    },
        new ScalarFunction() {
      @Override
      public RelDataType getReturnType(RelDataTypeFactory relDataTypeFactory) {
        return relDataTypeFactory.createSqlType(SqlTypeName.TIMESTAMP);
      }

      @Override
      public List<FunctionParameter> getParameters() {
        return List.of(new FunctionParameter() {
          @Override
          public int getOrdinal() {
            return 0;
          }

          @Override
          public String getName() {
            return "time";
          }

          @Override
          public RelDataType getType(RelDataTypeFactory relDataTypeFactory) {
            return relDataTypeFactory.createSqlType(SqlTypeName.TIMESTAMP);
          }

          @Override
          public boolean isOptional() {
            return false;
          }
        });
      }
    }
        );
  }

  @Override
  public Set<String> getFunctionNames() {
    return Set.of("now", "time_roundToMonth");
  }

  @Override
  public Schema getSubSchema(String s) {
    return null;
  }

  @Override
  public Set<String> getSubSchemaNames() {
    return Set.of();
  }

  @Override
  public Expression getExpression(SchemaPlus schemaPlus, String s) {
    return null;
  }

  @Override
  public boolean isMutable() {
    return false;
  }

  @Override
  public Schema snapshot(SchemaVersion schemaVersion) {
    return null;
  }
}
