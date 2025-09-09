/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.util;

import com.datasqrl.calcite.SqrlRexUtil;
import com.datasqrl.function.InputPreservingFunction;
import com.google.common.base.Preconditions;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.Value;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableList;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.planner.plan.schema.TimeIndicatorRelDataType;

public class CalciteUtil {

  public static boolean isNestedTable(RelDataType type) {
    return getNestedTableType(type).isPresent();
  }

  public static boolean isRowTime(RelDataType type) {
    return type instanceof TimeIndicatorRelDataType;
  }

  /**
   * Returns true if the LogicalProject does not alter the input — i.e., it is a trivial identity
   * projection. This is the case when all project expressions are RexInputRef(i) in order.
   */
  public static boolean isTrivialProject(RelNode relNode) {
    if (!(relNode instanceof LogicalProject project)) return false;
    var projects = project.getProjects();
    var inputFieldCount = project.getInput().getRowType().getFieldCount();

    if (projects.size() != inputFieldCount) {
      return false;
    }

    for (int i = 0; i < projects.size(); i++) {
      RexNode expr = projects.get(i);
      if (!(expr instanceof RexInputRef inputRef) || inputRef.getIndex() != i) {
        return false;
      }
    }

    return true;
  }

  public static Optional<Integer> findBestRowTimeIndex(RelDataType type) {
    return type.getFieldList().stream()
        .filter(field -> isRowTime(field.getType()))
        // Prioritize not null fields, then sort by index increasing
        .sorted(
            Comparator.comparing((RelDataTypeField field) -> field.getType().isNullable())
                .thenComparing(RelDataTypeField::getIndex))
        .map(RelDataTypeField::getIndex)
        .findFirst();
  }

  public static Optional<RelDataType> getNestedTableType(RelDataType type) {
    if (type.isStruct()) {
      return Optional.of(type);
    }
    return getArrayElementType(type).filter(RelDataType::isStruct);
  }

  public static boolean hasNestedTable(RelDataType type) {
    if (!type.isStruct()) {
      return false;
    }
    return type.getFieldList().stream()
        .map(RelDataTypeField::getType)
        .anyMatch(CalciteUtil::isNestedTable);
  }

  public static Function<Integer, String> getFieldName(RelDataType rowType) {
    final var names = rowType.getFieldNames();
    return i -> names.get(i);
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
    var fields = relBuilder.peek().getRowType().getFieldList();
    List<RexNode> projects = new ArrayList<>(fields.size());
    for (var i = 0; i < fields.size(); i++) {
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

  public static RelDataType addField(
      @NonNull RelDataType relation,
      int atIndex,
      @NonNull String fieldId,
      @NonNull RelDataType fieldType,
      @NonNull RelDataTypeFactory factory) {
    Preconditions.checkArgument(relation.isStruct());
    var builder = getRelTypeBuilder(factory);
    var index = 0;
    if (index == atIndex) {
      builder.add(fieldId, fieldType);
    }
    for (RelDataTypeField field : relation.getFieldList()) {
      builder.add(field);
      index++;
      if (index == atIndex) {
        builder.add(fieldId, fieldType);
      }
    }
    Preconditions.checkArgument(
        index >= atIndex, "Provided index [%s] larger than length [%s]", atIndex, index);
    return builder.build();
  }

  public static List<String> identifyNullableFields(RelDataType datatype, List<Integer> indexes) {
    List<String> fieldNames = new ArrayList<>();
    var fields = datatype.getFieldList();
    for (int index : indexes) {
      var field = fields.get(index);
      if (field.getType().isNullable()) {
        fieldNames.add(field.getName());
      }
    }
    return fieldNames;
  }

  public static Optional<Integer> getInputRef(RexNode rexNode) {
    if (rexNode instanceof RexInputRef) { // Direct mapping
      return Optional.of(((RexInputRef) rexNode).getIndex());
    }
    return Optional.empty();
  }

  /**
   * Determines the original index of the column with non-altering transformations.
   *
   * @param rexNode
   * @return
   */
  public static Optional<Integer> getNonAlteredInputRef(RexNode rexNode) {
    return getInputRefThroughTransform(
        rexNode, List.of(CAST_TRANSFORM, COALESCE_TRANSFORM, INPUT_PRESERVING_TRANSFORM));
  }

  public static Optional<Integer> getInputRefThroughTransform(
      RexNode rexNode, List<InputRefTransformation> transformations) {
    if (rexNode instanceof RexInputRef) { // Direct mapping
      return Optional.of(((RexInputRef) rexNode).getIndex());
    } else if (rexNode instanceof RexCall call) {
      var operator = call.getOperator();
      var operands = call.getOperands();
      Optional<InputRefTransformation> transform =
          StreamUtil.getOnlyElement(transformations.stream().filter(t -> t.appliesTo(operator)));
      return transform
          .filter(t -> t.validate(operator, operands))
          .flatMap(t -> getNonAlteredInputRef(t.getOperand(operator, operands)));
    }
    return Optional.empty();
  }

  public static Optional<Integer> getLimit(RelNode relNode) {
    Optional<Integer> limit = Optional.empty();
    if (relNode instanceof Sort sort) {
      limit = SqrlRexUtil.getLimit(sort.fetch);
    }
    return limit;
  }

  public interface InputRefTransformation {

    boolean appliesTo(SqlOperator operator);

    boolean validate(SqlOperator operator, List<RexNode> operands);

    RexNode getOperand(SqlOperator operator, List<RexNode> operands);
  }

  public static final InputRefTransformation INPUT_PRESERVING_TRANSFORM =
      new InputRefTransformation() {
        @Override
        public boolean appliesTo(SqlOperator operator) {
          return FunctionUtil.getBridgedFunction(operator)
              .flatMap(CalciteUtil::getInputPreservingFunction)
              .isPresent();
        }

        @Override
        public boolean validate(SqlOperator operator, List<RexNode> operands) {
          return true;
        }

        @Override
        public RexNode getOperand(SqlOperator operator, List<RexNode> operands) {
          var index =
              FunctionUtil.getBridgedFunction(operator)
                  .flatMap(CalciteUtil::getInputPreservingFunction)
                  .get()
                  .preservedOperandIndex();
          return operands.get(index);
        }
      };

  private static Optional<InputPreservingFunction> getInputPreservingFunction(
      FunctionDefinition functionDefinition) {
    return FunctionUtil.getFunctionMetaData(functionDefinition, InputPreservingFunction.class);
  }

  public static final InputRefTransformation COALESCE_TRANSFORM =
      new BasicInputRefTransformation(
          "coalesce", ops -> ops.size() == 2 && (ops.get(1) instanceof RexLiteral), 0);
  public static final InputRefTransformation CAST_TRANSFORM =
      new BasicInputRefTransformation("cast", ops -> ops.size() == 1, 0);

  @Value
  public static class BasicInputRefTransformation implements InputRefTransformation {

    String name;
    Predicate<List<RexNode>> validationFunction;
    int operandIndex;

    @Override
    public boolean appliesTo(SqlOperator operator) {
      return isOperator(operator, name);
    }

    @Override
    public boolean validate(SqlOperator operator, List<RexNode> operands) {
      return validationFunction.test(operands);
    }

    @Override
    public RexNode getOperand(SqlOperator operator, List<RexNode> operands) {
      return operands.get(operandIndex);
    }
  }

  public static boolean isOperator(SqlOperator operator, String operatorName) {
    return operator.isName(operatorName, false);
  }

  public static Optional<Integer> isEqualToConstant(RexNode rexNode) {
    int arity;
    if (rexNode.isA(SqlKind.EQUALS)) {
      arity = 2;
    } else if (rexNode.isA(SqlKind.IS_NULL)) {
      arity = 1;
    } else {
      return Optional.empty();
    }

    var operands = ((RexCall) rexNode).getOperands();
    assert arity == 1 || arity == 2;
    if (arity == 1 || isConstant(operands.get(1))) {
      return getInputRefThroughTransform(operands.get(0), List.of(CAST_TRANSFORM));
    } else if (arity == 2 && isConstant(operands.get(0))) {
      return getInputRefThroughTransform(operands.get(1), List.of(CAST_TRANSFORM));
    }
    return Optional.empty();
  }

  public static int indexOf(String columnName, RelDataType type) {
    RelDataTypeField field = type.getField(columnName, false, false);
    if (field == null) {
      return -1;
    } else {
      return field.getIndex();
    }
  }

  public static boolean isConstant(RexNode rexNode) {
    if (rexNode instanceof RexLiteral || rexNode instanceof RexDynamicParam) {
      return true;
    }
    if (rexNode instanceof RexFieldAccess) {
      if (((RexFieldAccess) rexNode).getReferenceExpr() instanceof RexCorrelVariable) {
        return true;
      }
    }
    return false;
  }

  public static void addProjection(
      @NonNull RelBuilder relBuilder, @NonNull List<Integer> selectIdx, List<String> fieldNames) {
    addProjection(relBuilder, selectIdx, fieldNames, false);
  }

  public static List<RexNode> getIdentityRex(@NonNull RelBuilder relBuilder, @NonNull int firstN) {
    return getSelectRex(
        relBuilder, IntStream.range(0, firstN).boxed().collect(Collectors.toUnmodifiableList()));
  }

  public static List<RexNode> getSelectRex(
      @NonNull RelBuilder relBuilder, @NonNull List<Integer> selectIdx) {
    Preconditions.checkArgument(!selectIdx.isEmpty());
    List<RexNode> rexList = new ArrayList<>(selectIdx.size());
    var inputType = relBuilder.peek().getRowType();
    selectIdx.forEach(idx -> rexList.add(RexInputRef.of(idx, inputType)));
    return rexList;
  }

  public static void addColumn(
      @NonNull RelBuilder relBuilder, @NonNull RexNode rexNode, @NonNull String columnName) {
    var cols = relBuilder.peek().getRowType().getFieldCount();
    var selects = CalciteUtil.getIdentityRex(relBuilder, cols);
    selects.add(rexNode);
    List<String> fieldNames =
        IntStream.range(0, cols + 1)
            .mapToObj(i -> i < cols ? null : columnName)
            .collect(Collectors.toList());
    assert selects.size() == fieldNames.size();
    relBuilder.project(selects, fieldNames);
  }

  public static void addProjection(
      @NonNull RelBuilder relBuilder,
      @NonNull List<Integer> selectIdx,
      List<String> fieldNames,
      boolean force) {
    if (fieldNames == null || fieldNames.isEmpty()) {
      fieldNames = Collections.nCopies(selectIdx.size(), null);
    }
    Preconditions.checkArgument(selectIdx.size() == fieldNames.size());
    relBuilder.project(
        getSelectRex(relBuilder, selectIdx),
        fieldNames,
        force); // Need to force otherwise Calcite eliminates the project
  }

  @Value
  public static class RelDataTypeFieldBuilder implements RelDataTypeBuilder {

    private final RelDataTypeFactory.FieldInfoBuilder fieldBuilder;

    @Override
    public RelDataTypeBuilder add(String name, RelDataType type) {
      return add(name, type, type.isNullable());
    }

    @Override
    public RelDataTypeBuilder add(String name, RelDataType type, boolean nullable) {
      fieldBuilder.add(name, type).nullable(nullable);
      return this;
    }

    @Override
    public RelDataTypeBuilder add(RelDataTypeField field) {
      return add(field.getName(), field.getType());
    }

    @Override
    public int getFieldCount() {
      return fieldBuilder.getFieldCount();
    }

    @Override
    public RelDataType build() {
      return fieldBuilder.build();
    }
  }

  public static RelNode applyRexShuttleRecursively(
      @NonNull RelNode node, @NonNull final RexShuttle rexShuttle) {
    return node.accept(new RexShuttleApplier(rexShuttle));
  }

  @AllArgsConstructor
  private static class RexShuttleApplier extends RelShuttleImpl {

    RexShuttle rexShuttle;

    @Override
    protected RelNode visitChild(RelNode parent, int i, RelNode child) {
      if (i == 0) {
        parent = parent.accept(rexShuttle);
      }
      return super.visitChild(parent, i, child);
    }
  }

  public static RelNode replaceParameters(
      @NonNull RelNode node, @NonNull List<RexNode> parameters) {
    return applyRexShuttleRecursively(node, new RexParameterReplacer(parameters, node));
  }

  @AllArgsConstructor
  private static class RexParameterReplacer extends RexShuttle {

    private final List<RexNode> parameters;
    private final RelNode relNode;

    @Override
    public RexNode visitDynamicParam(RexDynamicParam dynamicParam) {
      var index = dynamicParam.getIndex();
      Preconditions.checkArgument(
          index >= 0 && index < parameters.size(),
          "Query parameter index [%s] is out of bounds [%s] in: %s",
          relNode);
      return parameters.get(index);
    }
  }

  public static RexNode makeTimeInterval(long interval_ms, RexBuilder rexBuilder) {
    var intervalStr = Long.toString(interval_ms);
    var sqlIntervalQualifier =
        new SqlIntervalQualifier(
            TimeUnit.SECOND,
            getPrecision(intervalStr),
            TimeUnit.SECOND,
            getFracPrecision(intervalStr),
            SqlParserPos.ZERO);
    return rexBuilder.makeIntervalLiteral(new BigDecimal(interval_ms), sqlIntervalQualifier);
  }

  public static int getFracPrecision(String toValue) {
    var val = toValue.split("\\.");
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
      var type = typeFactory.createTypeWithNullability(call.getType(), false);
      return AggregateCall.create(
          call.getAggregation(),
          call.isDistinct(),
          call.isApproximate(),
          call.ignoreNulls(),
          call.getArgList(),
          call.filterArg,
          call.collation,
          type,
          call.name);
    } else {
      return call;
    }
  }

  /**
   * If the given RexNode is a comparison with the literal '0', this method returns the RexNode on
   * the other side of the comparison and an integer that
   *
   * @param rexNode
   * @return
   */
  public static Optional<RexNode> isGreaterZero(RexNode rexNode) {
    if (rexNode.isA(SqlKind.BINARY_COMPARISON)) {
      Preconditions.checkArgument(rexNode instanceof RexCall);
      var operands = ((RexCall) rexNode).getOperands();
      Preconditions.checkArgument(operands.size() == 2);
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
    if ((rexNode instanceof RexLiteral literal) && !RexLiteral.isNullLiteral(rexNode)) {
      return RexLiteral.intValue(literal) == 0;
    }
    return false;
  }

  public static void addFilteredDeduplication(
      RelBuilder relB, int timestampIdx, List<Integer> partition, int orderColIdx) {
    Preconditions.checkArgument(
        !partition.contains(timestampIdx), "Timestamp column cannot be part of the partition");
    Preconditions.checkArgument(
        !partition.contains(orderColIdx), "Order column cannot be part of the partition");
    var rexBuilder = relB.getCluster().getRexBuilder();

    var partitionKeys = getSelectRex(relB, partition);
    var fieldNames = relB.peek().getRowType().getFieldNames();
    var numFields = fieldNames.size();
    List<Integer> originalFieldIdx =
        IntStream.range(0, numFields).boxed().collect(Collectors.toUnmodifiableList());

    // MAX function over the window
    var rexOver =
        rexBuilder.makeOver(
            relB.field(orderColIdx).getType(),
            SqlStdOperatorTable.MAX,
            ImmutableList.of(relB.field(orderColIdx)),
            partitionKeys,
            ImmutableList.of(new RexFieldCollation(relB.field(timestampIdx), Set.of())),
            RexWindowBounds.UNBOUNDED_PRECEDING,
            RexWindowBounds.CURRENT_ROW,
            false,
            true,
            false,
            false,
            true);

    // Adding projection that computes a running max for the order col
    List<RexNode> projects = new ArrayList<>(relB.fields());
    projects.add(rexOver);
    relB.project(projects);

    // Filter out rows that aren't bigger or equal to the running max for order col (i.e. out of
    // order data)
    var condition =
        relB.call(
            SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
            relB.field(orderColIdx),
            relB.field(numFields));
    relB.filter(condition);

    // Compute the lag for all columns other than the order and timestamp column, ordered by
    // timestamp
    Function<Integer, RexNode> lagFunction =
        idx ->
            rexBuilder.makeOver(
                rexBuilder
                    .getTypeFactory()
                    .createTypeWithNullability(relB.field(idx).getType(), true),
                FlinkSqlOperatorTable.LAG,
                ImmutableList.of(relB.field(idx), rexBuilder.makeExactLiteral(BigDecimal.ONE)),
                partitionKeys,
                ImmutableList.of(new RexFieldCollation(relB.field(timestampIdx), Set.of())),
                RexWindowBounds.UNBOUNDED_PRECEDING,
                RexWindowBounds.CURRENT_ROW,
                false,
                true,
                false,
                false,
                false);

    projects = new ArrayList<>(relB.fields(originalFieldIdx));
    Map<Integer, Integer> lagPairs = new HashMap<>();
    var offset = numFields;
    for (var i = 0; i < numFields; i++) {
      if (i == orderColIdx || i == timestampIdx || partition.contains(i)) {
        continue;
      }
      projects.add(lagFunction.apply(i));
      lagPairs.put(i, offset++);
    }
    relB.project(projects);
    Preconditions.checkArgument(
        !lagPairs.isEmpty(), "Expected at least column that's not a partition key or timestamp");

    // Filter out records where all column values other than timestamps, order column, and partition
    // columns are identical
    List<RexNode> anyColumnDifferent = new ArrayList<>();
    // unless it's the first record which we check by looking if all lag columns are null
    var isFirstRow =
        relB.and(
            lagPairs.values().stream()
                .map(idx -> relB.isNull(relB.field(idx)))
                .collect(Collectors.toList()));
    anyColumnDifferent.add(isFirstRow);
    for (var i = 0; i < numFields; i++) {
      if (i == orderColIdx || i == timestampIdx || partition.contains(i)) {
        continue;
      }
      RexNode col = relB.field(i);
      Preconditions.checkArgument(
          !CalciteUtil.isNestedTable(col.getType()),
          "Filtered distinct not supported on nested data [column %s]",
          fieldNames.get(i));
      RexNode colLag = relB.field(lagPairs.get(i));

      var notEqualCondition = relB.call(SqlStdOperatorTable.NOT_EQUALS, col, colLag);
      anyColumnDifferent.add(notEqualCondition);
      if (col.getType().isNullable()) {
        anyColumnDifferent.add(relB.and(List.of(relB.isNull(col), relB.isNotNull(colLag))));
        anyColumnDifferent.add(relB.and(List.of(relB.isNotNull(col), relB.isNull(colLag))));
      }
    }
    relB.filter(relB.or(anyColumnDifferent));
    // Project to original fields
    relB.project(relB.fields(originalFieldIdx));
  }

  public static boolean isPotentialPrimaryKeyType(RelDataType type) {
    if (type.isNullable() || !(type instanceof BasicSqlType)) {
      return false;
    }
    var sqlType = type.getSqlTypeName();
    return switch (sqlType) {
      case CHAR,
              VARCHAR,
              BOOLEAN,
              TINYINT,
              SMALLINT,
              INTEGER,
              BIGINT,
              FLOAT,
              REAL,
              DOUBLE,
              DATE,
              TIME,
              TIMESTAMP,
              TIMESTAMP_WITH_LOCAL_TIME_ZONE,
              TIME_WITH_LOCAL_TIME_ZONE,
              DECIMAL ->
          true;
      default -> false;
    };
  }
}
