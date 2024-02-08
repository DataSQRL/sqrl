/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.ContiguousSet;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.Value;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.IntervalSqlType;
import org.apache.calcite.sql.type.MultisetSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.commons.lang3.tuple.Pair;

public class CalciteUtil {

  public static boolean isNestedTable(RelDataType type) {
    return getNestedTableType(type).isPresent();
  }

  public static Optional<RelDataType> getNestedTableType(RelDataType type) {
    if (type.isStruct()) return Optional.of(type);
    return getArrayElementType(type).filter(RelDataType::isStruct);
  }

  public static boolean isPrimitiveType(RelDataType t) {
    return !t.isStruct();
  }

  public static Optional<RelDataType> getArrayElementType(RelDataType type) {
    if (isArray(type)) {
      return Optional.of(type.getComponentType());
    } else {
      return Optional.empty();
    }
  }

  public static RelBuilder projectOutNested(RelBuilder relBuilder) {
    List<RelDataTypeField> fields = relBuilder.peek().getRowType().getFieldList();
    List<RexNode> projects = new ArrayList<>(fields.size());
    for (int i = 0; i < fields.size(); i++) {
      if (!CalciteUtil.isNestedTable(fields.get(i).getType())) {
        projects.add(relBuilder.field(i));
      }
    }
    relBuilder.project(projects);
    return relBuilder;
  }

  public static boolean isArray(RelDataType type) {
    return type instanceof ArraySqlType;
  }

  public static boolean isTimestamp(RelDataType datatype) {
    return !datatype.isStruct()
        && datatype.getSqlTypeName() == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE
        && !datatype.isNullable();
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

  /**
   * Determines the original index of the column with non-altering transformations.
   * @param rexNode
   * @return
   */
  public static Optional<Integer> getInputRefRobust(RexNode rexNode) {
    if (rexNode instanceof RexInputRef) { //Direct mapping
      return Optional.of(((RexInputRef) rexNode).getIndex());
    } else if (rexNode instanceof RexCall) {
      RexCall call = (RexCall)rexNode;
      List<RexNode> operands = call.getOperands();
      if (call.getOperator().isName("coalesce", false)) {
        if (operands.size()!=2) return Optional.empty();
        if (!(operands.get(1) instanceof RexLiteral)) return Optional.empty();
        return getInputRefRobust(operands.get(0));
      } else if (call.getOperator().isName("cast", false)) {
        if (operands.size()!=2) return Optional.empty();
        return getInputRefRobust(operands.get(0));
      }
    }
    return Optional.empty();
  }


  public static boolean isGreatestOf(RexNode rexNode, Set<Integer> columnIndexes) {
    if (!(rexNode instanceof RexCall)) return false;
    RexCall call = (RexCall) rexNode;
    if (call.getOperator().isName("greatest", false)) {
      Set<Integer> greatestSet = new HashSet<>();
      for (RexNode operand : call.getOperands()) {
        Optional<Integer> ref = getInputRefRobust(operand);
        if (ref.isEmpty()) return false;
        greatestSet.add(ref.get());
      }
      return columnIndexes.equals(greatestSet);
    }
    return false;
  }

  public static Optional<Integer> isEqualToConstant(RexNode rexNode) {
    if (!rexNode.isA(SqlKind.EQUALS) && !rexNode.isA(SqlKind.IS_NULL)) return Optional.empty();
    List<RexNode> operands = ((RexCall) rexNode).getOperands();
    if (!(operands.get(0) instanceof RexInputRef)) return Optional.empty();
    RexInputRef ref = (RexInputRef) operands.get(0);
    if (rexNode.isA(SqlKind.EQUALS) &&
        !isConstant(operands.get(1))) return Optional.empty();
    return Optional.of(ref.getIndex());
  }

  public static boolean isConstant(RexNode rexNode) {
    return rexNode instanceof RexLiteral || rexNode instanceof RexDynamicParam;
  }

  public static void addProjection(@NonNull RelBuilder relBuilder, @NonNull List<Integer> selectIdx,
      List<String> fieldNames) {
    addProjection(relBuilder, selectIdx, fieldNames, false);
  }

  public static List<RexNode> getSelectRex(@NonNull RelBuilder relBuilder, @NonNull List<Integer> selectIdx) {
    Preconditions.checkArgument(!selectIdx.isEmpty());
    List<RexNode> rexList = new ArrayList<>(selectIdx.size());
    RelDataType inputType = relBuilder.peek().getRowType();
    selectIdx.forEach(idx -> rexList.add(RexInputRef.of(idx, inputType)));
    return rexList;
  }

  public static void addProjection(@NonNull RelBuilder relBuilder, @NonNull List<Integer> selectIdx,
      List<String> fieldNames, boolean force) {
    if (fieldNames == null || fieldNames.isEmpty()) {
      fieldNames = Collections.nCopies(selectIdx.size(), null);
    }
    Preconditions.checkArgument(selectIdx.size() == fieldNames.size());
    relBuilder.project(getSelectRex(relBuilder, selectIdx), fieldNames,
        force); //Need to force otherwise Calcite eliminates the project
  }

  @Value
  public static class RelDataTypeFieldBuilder implements RelDataTypeBuilder {

    private final RelDataTypeFactory.FieldInfoBuilder fieldBuilder;

    public RelDataTypeBuilder add(String name, RelDataType type) {
      return add(name, type, type.isNullable());
    }

    public RelDataTypeBuilder add(String name, RelDataType type, boolean nullable) {
      fieldBuilder.add(name, type).nullable(nullable);
      return this;
    }

    public RelDataTypeBuilder add(RelDataTypeField field) {
      return add(field.getName(), field.getType());
    }

    @Override
    public int getFieldCount() {
      return fieldBuilder.getFieldCount();
    }

    public RelDataType build() {
      return fieldBuilder.build();
    }
  }

  public static RelNode applyRexShuttleRecursively(@NonNull RelNode node,
      @NonNull final RexShuttle rexShuttle) {
    return node.accept(new RexShuttleApplier(rexShuttle));
  }

  @AllArgsConstructor
  private static class RexShuttleApplier extends RelShuttleImpl {

    RexShuttle rexShuttle;

    @Override
    protected RelNode visitChild(RelNode parent, int i, RelNode child) {
      return super.visitChild(parent.accept(rexShuttle), i, child);
    }
  }

  public static RelNode replaceParameters(@NonNull RelNode node, @NonNull List<RexNode> parameters) {
    return applyRexShuttleRecursively(node, new RexParameterReplacer(parameters, node));
  }

  @AllArgsConstructor
  private static class RexParameterReplacer extends RexShuttle {

    private final List<RexNode> parameters;
    private final RelNode relNode;

    public RexNode visitDynamicParam(RexDynamicParam dynamicParam) {
      int index = dynamicParam.getIndex();
      Preconditions.checkArgument(index>=0 && index<parameters.size(),
          "Query parameter index [%s] is out of bounds [%s] in: %s", relNode);
      return parameters.get(index);
    }

  }

  public static RexNode makeTimeInterval(long interval_ms, RexBuilder rexBuilder) {
    String intervalStr = Long.toString(interval_ms);
    SqlIntervalQualifier sqlIntervalQualifier =
        new SqlIntervalQualifier(TimeUnit.SECOND, getPrecision(intervalStr), TimeUnit.SECOND,
            getFracPrecision(intervalStr),
            SqlParserPos.ZERO);
    return rexBuilder.makeIntervalLiteral(new BigDecimal(interval_ms), sqlIntervalQualifier);
  }

  public static int getFracPrecision(String toValue) {
    String[] val = toValue.split("\\.");
    if (val.length == 2) {
      return val[1].length();
    }

    return -1;
  }

  public static int getPrecision(String toValue) {
    return toValue.split("\\.")[0].length();
  }

  public static AggregateCall makeNotNull(AggregateCall call, RelDataTypeFactory typeFactory) {
    if (call.getType().isNullable()) {
      RelDataType type = typeFactory.createTypeWithNullability(call.getType(), false);
      return AggregateCall.create(call.getAggregation(), call.isDistinct(), call.isApproximate(), call.ignoreNulls(), call.getArgList(),
          call.filterArg, call.collation, type, call.name);
    } else {
      return call;
    }
  }

  /**
   * If the given RexNode is a comparison with the literal '0', this method returns
   * the RexNode on the other side of the comparison and an integer that
   *
   * @param rexNode
   * @return
   */
  public static Optional<RexNode> isGreaterZero(RexNode rexNode) {
    if (rexNode.isA(SqlKind.BINARY_COMPARISON)) {
      Preconditions.checkArgument(rexNode instanceof RexCall);
      List<RexNode> operands = ((RexCall)rexNode).getOperands();
      Preconditions.checkArgument(operands.size()==2);
      if (isZero(operands.get(0))) {
        if (rexNode.isA(SqlKind.NOT_EQUALS) || rexNode.isA(SqlKind.LESS_THAN)) {
          return Optional.of(operands.get(1));
        }
      } else if (isZero(operands.get(1))) {
        if (rexNode.isA(SqlKind.NOT_EQUALS) || rexNode.isA(SqlKind.GREATER_THAN)) {
          return Optional.of(operands.get(0));
        }
      }
    }
    return Optional.empty();
  }

  public static boolean isZero(RexNode rexNode) {
    if ((rexNode instanceof RexLiteral) && !RexLiteral.isNullLiteral(rexNode)) {
      RexLiteral literal = (RexLiteral) rexNode;
      return RexLiteral.intValue(literal) == 0;
    }
    return false;
  }

}
