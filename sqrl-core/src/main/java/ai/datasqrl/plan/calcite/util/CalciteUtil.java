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
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.AggregatingScope;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqrlValidatorImpl;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
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

  public static List<SqlNode> getHintOptions(SqlHint hint) {
    SqlNodeList nodeList = (SqlNodeList)hint.getOperandList().get(1);
    if (nodeList == null) return List.of();
    return nodeList.getList();
  }

  public static void appendSelectListItem(SqlSelect select, SqlNode node) {
    List<SqlNode> list = new ArrayList<>(select.getSelectList().getList());
    list.add(node);
    SqlNodeList nodeList = new SqlNodeList(list, select.getSelectList().getParserPosition());
    select.setOperand(1, nodeList);
  }


  public static boolean selectListExpressionEquals(SqlNode selectItem, SqlNode exp, SqlValidatorScope scope) {

    switch (selectItem.getKind()) {
      case AS:
        SqlCall call = (SqlCall) selectItem;
        if (selectListExpressionEquals(call.getOperandList().get(0), exp, scope)) {
          return true;
        }
        //intentional fallthrough
      default:
        if (exp.equalsDeep(selectItem, Litmus.IGNORE) ||
            (exp instanceof SqlIdentifier && scope.fullyQualify((SqlIdentifier) exp)
                .identifier.equalsDeep(selectItem, Litmus.IGNORE))
        ) {
          return true;
        }
    }
    return false;
  }

  public static void prependGroupByNodes(SqlSelect select, List<SqlNode> nodes) {
    if (nodes.isEmpty()) return;
    select.setOperand(4, prependToList(select.getGroup(), nodes));
  }

  public static void prependOrderByNodes(SqlSelect select, List<SqlNode> nodes) {
    if (nodes.isEmpty()) return;
    select.setOperand(7, prependToList(select.getOrderList(), nodes));
  }

  public static SqlNodeList prependToList(SqlNodeList list, List<SqlNode> nodes) {
    List<SqlNode> newGroup = new ArrayList<>(nodes);
    if (list != null) {
      newGroup.addAll(list.getList());
    }
    return new SqlNodeList(newGroup, nodes.get(0).getParserPosition());
  }

  public static void removeKeywords(SqlSelect select) {
    select.setOperand(0, SqlNodeList.EMPTY);
  }

  public static List<SqlIdentifier> getColumnNames(SqlSelect select, SqlValidatorScope scope) {
    //Get names from select list directly
    return select.getSelectList().getList().stream()
        .map(i -> toIdentifierName(i))
        .collect(Collectors.toList());
  }

  private static SqlIdentifier toIdentifierName(SqlNode node) {
    switch (node.getKind()) {
      case AS:
        SqlCall call = (SqlCall) node;
        SqlNode alias = call.getOperandList().get(1);
        return (SqlIdentifier) alias;
      default:
        SqlIdentifier identifier = (SqlIdentifier) node;
        String name = Util.last(identifier.names);
        return new SqlIdentifier(name, SqlParserPos.ZERO);
    }
  }

  public static void wrapSelectInProject(SqlSelect select, SqlValidatorScope scope) {
    SqlSelect innerSelect = (SqlSelect)select.clone(select.getParserPosition());

    List<SqlIdentifier> names = CalciteUtil.getColumnNames(select, scope);
    SqlNodeList columnNames = new SqlNodeList(names, select.getSelectList().getParserPosition());

    select.setOperand(0, SqlNodeList.EMPTY);
    select.setOperand(1, columnNames);
    select.setOperand(2, innerSelect);
    select.setOperand(3, null);
    select.setOperand(4, null);
    select.setOperand(5, null);
    select.setOperand(6, SqlNodeList.EMPTY);
    select.setOperand(7, null);
    select.setOperand(8, null);
    select.setOperand(9, null);
  }

  public static void setHint(SqlSelect select, SqlHint hint) {
    select.setHints(new SqlNodeList(List.of(hint), SqlParserPos.ZERO));
  }

  public static boolean deepContainsNodeName(List<SqlNode> nodes, SqlNode sqlNode) {
    for (SqlNode node : nodes) {
      if (sqlNode.getKind() == SqlKind.AS) {
        SqlCall call = (SqlCall) sqlNode;
        if (call.getOperandList().get(0).equalsDeep(node, Litmus.IGNORE)) {
          return true;
        }
        if (call.getOperandList().get(1).equalsDeep(node, Litmus.IGNORE)) {
          return true;
        }
      }

      if (node.equalsDeep(sqlNode, Litmus.IGNORE)) {
        return true;
      }
    }
    return false;
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
    addIdentityProjection(relBuilder,numColumns,Set.of());
  }

  public static void addIdentityProjection(RelBuilder relBuilder, int numColumns, Set<Integer> skipIndexes) {
    List<RexNode> rex = new ArrayList<>(numColumns);
    List<String> fieldNames = new ArrayList<>(numColumns);
    RelDataType inputType = relBuilder.peek().getRowType();
    for (int i = 0; i < numColumns; i++) {
      if (skipIndexes.contains(i)) continue;
      rex.add(i, RexInputRef.of(i,inputType));
      fieldNames.add(i,null);
    }
    Preconditions.checkArgument(!rex.isEmpty(),"No columns slected");
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
