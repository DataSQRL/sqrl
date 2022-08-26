package ai.datasqrl.plan.calcite.util;

import ai.datasqrl.parse.tree.name.Name;
import com.google.common.base.Preconditions;
import lombok.NonNull;
import lombok.Value;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.AggregatingScope;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqrlValidatorImpl;
import org.apache.calcite.tools.RelBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class CalciteUtil {

  public static boolean isNestedTable(RelDataType type) {
    if (type.isStruct()) {
      return true;
    }
    return getArrayElementType(type).map(RelDataType::isStruct).orElse(false);
  }

  public static Optional<RelDataType> getArrayElementType(RelDataType type) {
    if (isArray(type)) {
      return Optional.of(type.getComponentType());
    } else {
      return Optional.empty();
    }
  }

  public static boolean hasNesting(RelDataType type) {
    Preconditions.checkState(type.getFieldCount() > 0);
    return type.getFieldList().stream().map(t -> t.getType()).anyMatch(CalciteUtil::isNestedTable);
  }

  public static boolean isArray(RelDataType type) {
    return type instanceof ArraySqlType;
  }

  public static boolean isSingleUnnamedColumn(SqlNode query) {
    SqlSelect select = stripOrderBy(query);

    if (select.getSelectList().getList().size() != 1) {
      return false;
    }

    if (select.getSelectList().get(0).getKind() != SqlKind.AS) {
      return true;
    }

    return false;
  }

  public static boolean isTimestamp(RelDataType datatype) {
    return !datatype.isStruct() && datatype.getSqlTypeName() == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE
            && !datatype.isNullable();
  }

  private static SqlSelect stripOrderBy(SqlNode query) {
    if (query instanceof SqlSelect) {
      return (SqlSelect) query;
    } else if (query instanceof SqlOrderBy) {
      return (SqlSelect)((SqlOrderBy) query).query;
    }
    return null;
  }

  public static boolean isAggregating(SqlNode query, SqrlValidatorImpl sqrlValidator) {
    SqlValidatorScope scope = sqrlValidator.getSelectScope(stripOrderBy(query));

    return scope instanceof AggregatingScope;
  }

  public interface RelDataTypeBuilder {

    public default RelDataTypeBuilder add(Name name, RelDataType type) {
      return add(name.getCanonical(), type);
    }

    public RelDataTypeBuilder add(String name, RelDataType type);

    public default RelDataTypeBuilder add(Name name, RelDataType type, boolean nullable) {
      return add(name.getCanonical(), type, nullable);
    }

    public RelDataTypeBuilder add(String name, RelDataType type, boolean nullable);

    public RelDataTypeBuilder add(RelDataTypeField field);

    public default RelDataTypeBuilder addAll(Iterable<RelDataTypeField> fields) {
      for (RelDataTypeField field : fields) {
        add(field);
      }
      return this;
    }

    public RelDataType build();

  }

  public static RelDataTypeBuilder getRelTypeBuilder(@NonNull RelDataTypeFactory factory) {
    return new RelDataTypeFieldBuilder(factory.builder().kind(StructKind.FULLY_QUALIFIED));
  }

  public static RelDataType appendField(@NonNull RelDataType relation, @NonNull String fieldId,
      @NonNull RelDataType fieldType,
      @NonNull RelDataTypeFactory factory) {
    Preconditions.checkArgument(relation.isStruct());
    RelDataTypeBuilder builder = getRelTypeBuilder(factory);
    builder.addAll(relation.getFieldList());
    builder.add(fieldId, fieldType);
    return builder.build();
  }

  public static <C extends org.apache.calcite.schema.Table> List<C> getTables(CalciteSchema schema,
      Class<C> clazz) {
    return schema.getTableNames().stream()
        .map(s -> schema.getTable(s, true).getTable())
        .filter(clazz::isInstance).map(clazz::cast)
        .collect(Collectors.toList());
  }

  public static void addIdentityProjection(RelBuilder relBuilder, int numColumns) {
    List<RexNode> rex = new ArrayList<>(numColumns);
    List<String> fieldNames = new ArrayList<>(numColumns);
    RelDataType inputType = relBuilder.peek().getRowType();
    for (int i = 0; i < numColumns; i++) {
      rex.add(i, RexInputRef.of(i,inputType));
      fieldNames.add(i,null);
    }
    relBuilder.project(rex,fieldNames,true); //Need to force otherwise Calcite eliminates the project
  }

  @Value
  public static class RelDataTypeFieldBuilder implements RelDataTypeBuilder {

    private final RelDataTypeFactory.FieldInfoBuilder fieldBuilder;

    public RelDataTypeBuilder add(String name, RelDataType type) {
      fieldBuilder.add(name, type);
      return this;
    }

    public static RelDataType appendField(@NonNull RelDataType relation, @NonNull String fieldId, @NonNull RelDataType fieldType,
                                       @NonNull RelDataTypeFactory factory) {
      Preconditions.checkArgument(relation.isStruct());
      RelDataTypeBuilder builder = getRelTypeBuilder(factory);
      builder.addAll(relation.getFieldList());
      builder.add(fieldId, fieldType);
      return builder.build();

    }

    public RelDataTypeBuilder add(String name, RelDataType type, boolean nullable) {
      fieldBuilder.add(name, type).nullable(nullable);
      return this;
    }

    public RelDataTypeBuilder add(RelDataTypeField field) {
      //TODO: Do we need to do a deep clone or is this kosher since fields are immutable?
      fieldBuilder.add(field);
      return this;
    }

    public RelDataType build() {
      return fieldBuilder.build();
    }
  }
}
