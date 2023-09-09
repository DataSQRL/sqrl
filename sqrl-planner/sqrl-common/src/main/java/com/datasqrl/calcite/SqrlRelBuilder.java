package com.datasqrl.calcite;

import com.datasqrl.calcite.schema.ExpandTableMacroRule;
import com.datasqrl.calcite.schema.op.LogicalAddColumnOp;
import com.datasqrl.calcite.schema.op.LogicalAssignTimestampOp;
import com.datasqrl.calcite.schema.op.LogicalCreateReferenceOp;
import com.datasqrl.calcite.schema.op.LogicalCreateStreamOp;
import com.datasqrl.calcite.schema.op.LogicalExportOp;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.ReservedName;
import com.datasqrl.model.StreamType;
import com.datasqrl.plan.rel.LogicalStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.function.UnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptTable.ToRelContext;
import org.apache.calcite.plan.ViewExpanders;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.RelFactories.Struct;
import org.apache.calcite.rel.core.RelFactories.TableScanFactory;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqrlTableFunctionDef;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilder.AggCall;
import org.apache.calcite.tools.RelBuilder.Config;
import org.apache.calcite.tools.RelBuilder.GroupKey;
import org.apache.calcite.util.Holder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings.TargetMapping;
import org.apache.commons.collections.ListUtils;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableList;

public class SqrlRelBuilder {
  private final RelBuilder builder;
  private final CatalogReader catalogReader;
  private final QueryPlanner planner;
  private final Struct struct;
  private final ToRelContext viewExpander;

  public SqrlRelBuilder(RelBuilder relBuilder, CatalogReader catalogReader,
      QueryPlanner queryPlanner) {
    this.builder = relBuilder;
    this.catalogReader = catalogReader;
    this.planner = queryPlanner;
    this.struct = (RelFactories.Struct) Objects.requireNonNull(Struct.fromContext(Contexts.EMPTY_CONTEXT));
    this.viewExpander = ViewExpanders.simpleContext(relBuilder.getCluster());
  }

  public SqrlRelBuilder transform(UnaryOperator<Config> transform) {
    builder.transform(transform);
    return this;
  }

  public RelDataTypeFactory getTypeFactory() {
    return builder.getTypeFactory();
  }

  public SqrlRelBuilder adoptConvention(Convention convention) {
    builder.adoptConvention(convention);
    return this;
  }

  public RexBuilder getRexBuilder() {
    return builder.getRexBuilder();
  }

  public RelOptCluster getCluster() {
    return builder.getCluster();
  }

  public RelOptSchema getRelOptSchema() {
    return builder.getRelOptSchema();
  }

  public TableScanFactory getScanFactory() {
    return builder.getScanFactory();
  }

  public SqrlRelBuilder push(RelNode node) {
    builder.push(node);
    return this;
  }

  public SqrlRelBuilder pushAll(Iterable<? extends RelNode> nodes) {
    builder.pushAll(nodes);
    return this;
  }

  public RelNode build() {
    return builder.build();
  }

  public RelNode peek() {
    return builder.peek();
  }

  public RelNode peek(int n) {
    return builder.peek(n);
  }

  public RelNode peek(int inputCount, int inputOrdinal) {
    return builder.peek(inputCount, inputOrdinal);
  }

  public RexNode literal(Object value) {
    return builder.literal(value);
  }

  public SqrlRelBuilder variable(Holder<RexCorrelVariable> v) {
    builder.variable(v);
    return this;
  }

  public RexInputRef field(String fieldName) {
    return builder.field(fieldName);
  }

  public RexInputRef field(int inputCount, int inputOrdinal, String fieldName) {
    return builder.field(inputCount, inputOrdinal, fieldName);
  }

  public RexInputRef field(int fieldOrdinal) {
    return builder.field(fieldOrdinal);
  }

  public RexInputRef field(int inputCount, int inputOrdinal, int fieldOrdinal) {
    return builder.field(inputCount, inputOrdinal, fieldOrdinal);
  }

  public RexNode field(String alias, String fieldName) {
    return builder.field(alias, fieldName);
  }

  public RexNode field(int inputCount, String alias, String fieldName) {
    return builder.field(inputCount, alias, fieldName);
  }

  public RexNode field(RexNode e, String name) {
    return builder.field(e, name);
  }

  public ImmutableList<RexNode> fields() {
    return builder.fields();
  }

  public ImmutableList<RexNode> fields(int inputCount, int inputOrdinal) {
    return builder.fields(inputCount, inputOrdinal);
  }

  public ImmutableList<RexNode> fields(RelCollation collation) {
    return builder.fields(collation);
  }

  public ImmutableList<RexNode> fields(List<? extends Number> ordinals) {
    return builder.fields(ordinals);
  }

  public ImmutableList<RexNode> fields(ImmutableBitSet ordinals) {
    return builder.fields(ordinals);
  }

  public ImmutableList<RexNode> fields(Iterable<String> fieldNames) {
    return builder.fields(fieldNames);
  }

  public ImmutableList<RexNode> fields(TargetMapping mapping) {
    return builder.fields(mapping);
  }

  public RexNode dot(RexNode node, String fieldName) {
    return builder.dot(node, fieldName);
  }

  public RexNode dot(RexNode node, int fieldOrdinal) {
    return builder.dot(node, fieldOrdinal);
  }

  @Nonnull
  public RexNode call(SqlOperator operator, RexNode... operands) {
    return builder.call(operator, operands);
  }

  @Nonnull
  public RexNode call(SqlOperator operator, Iterable<? extends RexNode> operands) {
    return builder.call(operator, operands);
  }

  public RexNode in(RexNode arg, RexNode... ranges) {
    return builder.in(arg, ranges);
  }

  public RexNode in(RexNode arg, Iterable<? extends RexNode> ranges) {
    return builder.in(arg, ranges);
  }

  public RexNode and(RexNode... operands) {
    return builder.and(operands);
  }

  public RexNode and(Iterable<? extends RexNode> operands) {
    return builder.and(operands);
  }

  public RexNode or(RexNode... operands) {
    return builder.or(operands);
  }

  public RexNode or(Iterable<? extends RexNode> operands) {
    return builder.or(operands);
  }

  public RexNode not(RexNode operand) {
    return builder.not(operand);
  }

  public RexNode equals(RexNode operand0, RexNode operand1) {
    return builder.equals(operand0, operand1);
  }

  public RexNode notEquals(RexNode operand0, RexNode operand1) {
    return builder.notEquals(operand0, operand1);
  }

  public RexNode between(RexNode arg, RexNode lower, RexNode upper) {
    return builder.between(arg, lower, upper);
  }

  public RexNode isNull(RexNode operand) {
    return builder.isNull(operand);
  }

  public RexNode isNotNull(RexNode operand) {
    return builder.isNotNull(operand);
  }

  public RexNode cast(RexNode expr, SqlTypeName typeName) {
    return builder.cast(expr, typeName);
  }

  public RexNode cast(RexNode expr, SqlTypeName typeName, int precision) {
    return builder.cast(expr, typeName, precision);
  }

  public RexNode cast(RexNode expr, SqlTypeName typeName, int precision, int scale) {
    return builder.cast(expr, typeName, precision, scale);
  }

  public RexNode alias(RexNode expr, String alias) {
    return builder.alias(expr, alias);
  }

  public RexNode desc(RexNode node) {
    return builder.desc(node);
  }

  public RexNode nullsLast(RexNode node) {
    return builder.nullsLast(node);
  }

  public RexNode nullsFirst(RexNode node) {
    return builder.nullsFirst(node);
  }

  public GroupKey groupKey() {
    return builder.groupKey();
  }

  public GroupKey groupKey(RexNode... nodes) {
    return builder.groupKey(nodes);
  }

  public GroupKey groupKey(Iterable<? extends RexNode> nodes) {
    return builder.groupKey(nodes);
  }

  public GroupKey groupKey(Iterable<? extends RexNode> nodes,
      Iterable<? extends Iterable<? extends RexNode>> nodeLists) {
    return builder.groupKey(nodes, nodeLists);
  }

  @Deprecated
  public GroupKey groupKey(Iterable<? extends RexNode> nodes, boolean indicator,
      Iterable<? extends Iterable<? extends RexNode>> nodeLists) {
    return builder.groupKey(nodes, indicator, nodeLists);
  }

  public GroupKey groupKey(int... fieldOrdinals) {
    return builder.groupKey(fieldOrdinals);
  }

  public GroupKey groupKey(String... fieldNames) {
    return builder.groupKey(fieldNames);
  }

  public GroupKey groupKey(@Nonnull ImmutableBitSet groupSet) {
    return builder.groupKey(groupSet);
  }

  public GroupKey groupKey(ImmutableBitSet groupSet,
      @Nonnull Iterable<? extends ImmutableBitSet> groupSets) {
    return builder.groupKey(groupSet, groupSets);
  }

  @Deprecated
  public GroupKey groupKey(ImmutableBitSet groupSet, ImmutableList<ImmutableBitSet> groupSets) {
    return builder.groupKey(groupSet, groupSets);
  }

  @Deprecated
  public GroupKey groupKey(ImmutableBitSet groupSet, boolean indicator,
      ImmutableList<ImmutableBitSet> groupSets) {
    return builder.groupKey(groupSet, indicator, groupSets);
  }

  @Deprecated
  public AggCall aggregateCall(SqlAggFunction aggFunction, boolean distinct, RexNode filter,
      String alias, RexNode... operands) {
    return builder.aggregateCall(aggFunction, distinct, filter, alias, operands);
  }

  @Deprecated
  public AggCall aggregateCall(SqlAggFunction aggFunction, boolean distinct, boolean approximate,
      RexNode filter, String alias, RexNode... operands) {
    return builder.aggregateCall(aggFunction, distinct, approximate, filter, alias, operands);
  }

  @Deprecated
  public AggCall aggregateCall(SqlAggFunction aggFunction, boolean distinct, RexNode filter,
      String alias, Iterable<? extends RexNode> operands) {
    return builder.aggregateCall(aggFunction, distinct, filter, alias, operands);
  }

  @Deprecated
  public AggCall aggregateCall(SqlAggFunction aggFunction, boolean distinct, boolean approximate,
      RexNode filter, String alias, Iterable<? extends RexNode> operands) {
    return builder.aggregateCall(aggFunction, distinct, approximate, filter, alias, operands);
  }

  public AggCall aggregateCall(SqlAggFunction aggFunction, Iterable<? extends RexNode> operands) {
    return builder.aggregateCall(aggFunction, operands);
  }

  public AggCall aggregateCall(SqlAggFunction aggFunction, RexNode... operands) {
    return builder.aggregateCall(aggFunction, operands);
  }

  public AggCall aggregateCall(AggregateCall a) {
    return builder.aggregateCall(a);
  }

  public AggCall aggregateCall(AggregateCall a, Mapping mapping) {
    return builder.aggregateCall(a, mapping);
  }

  public AggCall count(RexNode... operands) {
    return builder.count(operands);
  }

  public AggCall count(Iterable<? extends RexNode> operands) {
    return builder.count(operands);
  }

  public AggCall count(boolean distinct, String alias, RexNode... operands) {
    return builder.count(distinct, alias, operands);
  }

  public AggCall count(boolean distinct, String alias, Iterable<? extends RexNode> operands) {
    return builder.count(distinct, alias, operands);
  }

  public AggCall countStar(String alias) {
    return builder.countStar(alias);
  }

  public AggCall sum(RexNode operand) {
    return builder.sum(operand);
  }

  public AggCall sum(boolean distinct, String alias, RexNode operand) {
    return builder.sum(distinct, alias, operand);
  }

  public AggCall avg(RexNode operand) {
    return builder.avg(operand);
  }

  public AggCall avg(boolean distinct, String alias, RexNode operand) {
    return builder.avg(distinct, alias, operand);
  }

  public AggCall min(RexNode operand) {
    return builder.min(operand);
  }

  public AggCall min(String alias, RexNode operand) {
    return builder.min(alias, operand);
  }

  public AggCall max(RexNode operand) {
    return builder.max(operand);
  }

  public AggCall max(String alias, RexNode operand) {
    return builder.max(alias, operand);
  }

  public RexNode patternField(String alpha, RelDataType type, int i) {
    return builder.patternField(alpha, type, i);
  }

  public RexNode patternConcat(Iterable<? extends RexNode> nodes) {
    return builder.patternConcat(nodes);
  }

  public RexNode patternConcat(RexNode... nodes) {
    return builder.patternConcat(nodes);
  }

  public RexNode patternAlter(Iterable<? extends RexNode> nodes) {
    return builder.patternAlter(nodes);
  }

  public RexNode patternAlter(RexNode... nodes) {
    return builder.patternAlter(nodes);
  }

  public RexNode patternQuantify(Iterable<? extends RexNode> nodes) {
    return builder.patternQuantify(nodes);
  }

  public RexNode patternQuantify(RexNode... nodes) {
    return builder.patternQuantify(nodes);
  }

  public RexNode patternPermute(Iterable<? extends RexNode> nodes) {
    return builder.patternPermute(nodes);
  }

  public RexNode patternPermute(RexNode... nodes) {
    return builder.patternPermute(nodes);
  }

  public RexNode patternExclude(RexNode node) {
    return builder.patternExclude(node);
  }

  public SqrlRelBuilder scan(Iterable<String> tableNames) {
    builder.scan(tableNames);
    return this;
  }

  public SqrlRelBuilder scan(String... tableNames) {
    builder.scan(tableNames);
    return this;
  }

  public SqrlRelBuilder snapshot(RexNode period) {
    builder.snapshot(period);
    return this;
  }

  public RexNode cursor(int inputCount, int ordinal) {
    return builder.cursor(inputCount, ordinal);
  }

  public SqrlRelBuilder functionScan(SqlOperator operator, int inputCount, RexNode... operands) {
    builder.functionScan(operator, inputCount, operands);
    return this;
  }

  public SqrlRelBuilder functionScan(SqlOperator operator, int inputCount,
      Iterable<? extends RexNode> operands) {
    builder.functionScan(operator, inputCount, operands);
    return this;
  }

  public SqrlRelBuilder filter(RexNode... predicates) {
    builder.filter(predicates);
    return this;
  }

  public SqrlRelBuilder filter(Iterable<? extends RexNode> predicates) {
    builder.filter(predicates);
    return this;
  }

  public SqrlRelBuilder filter(Iterable<CorrelationId> variablesSet, RexNode... predicates) {
    builder.filter(variablesSet, predicates);
    return this;
  }

  public SqrlRelBuilder filter(Iterable<CorrelationId> variablesSet,
      Iterable<? extends RexNode> predicates) {
    builder.filter(variablesSet, predicates);
    return this;
  }

  public SqrlRelBuilder project(RexNode... nodes) {
    builder.project(nodes);
    return this;
  }

  public SqrlRelBuilder project(Iterable<? extends RexNode> nodes) {
    builder.project(nodes);
    return this;
  }

  public SqrlRelBuilder project(Iterable<? extends RexNode> nodes, Iterable<String> fieldNames) {
    builder.project(nodes, fieldNames);
    return this;
  }

  public SqrlRelBuilder project(Iterable<? extends RexNode> nodes, Iterable<String> fieldNames,
      boolean force) {
    builder.project(nodes, fieldNames, force);
    return this;
  }

  public SqrlRelBuilder projectPlus(RexNode... nodes) {
    builder.projectPlus(nodes);
    return this;
  }

  public SqrlRelBuilder projectPlus(Iterable<RexNode> nodes) {
    builder.projectPlus(nodes);
    return this;
  }

  public SqrlRelBuilder projectExcept(RexNode... expressions) {
    builder.projectExcept(expressions);
    return this;
  }

  public SqrlRelBuilder projectExcept(Iterable<RexNode> expressions) {
    builder.projectExcept(expressions);
    return this;
  }

  public SqrlRelBuilder projectNamed(Iterable<? extends RexNode> nodes, Iterable<String> fieldNames,
      boolean force) {
    builder.projectNamed(nodes, fieldNames, force);
    return this;
  }

  public SqrlRelBuilder uncollect(List<String> itemAliases, boolean withOrdinality) {
    builder.uncollect(itemAliases, withOrdinality);
    return this;
  }

  public SqrlRelBuilder rename(List<String> fieldNames) {
    builder.rename(fieldNames);
    return this;
  }

  public SqrlRelBuilder distinct() {
    builder.distinct();
    return this;
  }

  public SqrlRelBuilder aggregate(GroupKey groupKey, AggCall... aggCalls) {
    builder.aggregate(groupKey, aggCalls);
    return this;
  }

  public SqrlRelBuilder aggregate(GroupKey groupKey, List<AggregateCall> aggregateCalls) {
    builder.aggregate(groupKey, aggregateCalls);
    return this;
  }

  public SqrlRelBuilder aggregate(GroupKey groupKey, Iterable<AggCall> aggCalls) {
    builder.aggregate(groupKey, aggCalls);
    return this;
  }

  public SqrlRelBuilder union(boolean all) {
    builder.union(all);
    return this;
  }

  public SqrlRelBuilder union(boolean all, int n) {
    builder.union(all, n);
    return this;
  }

  public SqrlRelBuilder intersect(boolean all) {
    builder.intersect(all);
    return this;
  }

  public SqrlRelBuilder intersect(boolean all, int n) {
    builder.intersect(all, n);
    return this;
  }

  public SqrlRelBuilder minus(boolean all) {
    builder.minus(all);
    return this;
  }

  public SqrlRelBuilder minus(boolean all, int n) {
    builder.minus(all, n);
    return this;
  }

  public SqrlRelBuilder transientScan(String tableName) {
    builder.transientScan(tableName);
    return this;
  }

  public SqrlRelBuilder transientScan(String tableName, RelDataType rowType) {
    builder.transientScan(tableName, rowType);
    return this;
  }

  public SqrlRelBuilder repeatUnion(String tableName, boolean all) {
    builder.repeatUnion(tableName, all);
    return this;
  }

  public SqrlRelBuilder repeatUnion(String tableName, boolean all, int iterationLimit) {
    builder.repeatUnion(tableName, all, iterationLimit);
    return this;
  }

  public SqrlRelBuilder join(JoinRelType joinType, RexNode condition0, RexNode... conditions) {
    builder.join(joinType, condition0, conditions);
    return this;
  }

  public SqrlRelBuilder join(JoinRelType joinType, Iterable<? extends RexNode> conditions) {
    builder.join(joinType, conditions);
    return this;
  }

  public SqrlRelBuilder join(JoinRelType joinType, RexNode condition) {
    builder.join(joinType, condition);
    return this;
  }

  public SqrlRelBuilder join(JoinRelType joinType, RexNode condition,
      Set<CorrelationId> variablesSet) {
    builder.join(joinType, condition, variablesSet);
    return this;
  }

  public SqrlRelBuilder correlate(JoinRelType joinType, CorrelationId correlationId,
      RexNode... requiredFields) {
    builder.correlate(joinType, correlationId, requiredFields);
    return this;
  }

  public SqrlRelBuilder correlate(JoinRelType joinType, CorrelationId correlationId,
      Iterable<? extends RexNode> requiredFields) {
    builder.correlate(joinType, correlationId, requiredFields);
    return this;
  }

  public SqrlRelBuilder join(JoinRelType joinType, String... fieldNames) {
    builder.join(joinType, fieldNames);
    return this;
  }

  public SqrlRelBuilder semiJoin(Iterable<? extends RexNode> conditions) {
    builder.semiJoin(conditions);
    return this;
  }

  public SqrlRelBuilder semiJoin(RexNode... conditions) {
    builder.semiJoin(conditions);
    return this;
  }

  public SqrlRelBuilder antiJoin(Iterable<? extends RexNode> conditions) {
    builder.antiJoin(conditions);
    return this;
  }

  public SqrlRelBuilder antiJoin(RexNode... conditions) {
    builder.antiJoin(conditions);
    return this;
  }

  public SqrlRelBuilder as(String alias) {
    builder.as(alias);
    return this;
  }

  public SqrlRelBuilder values(String[] fieldNames, Object... values) {
    builder.values(fieldNames, values);
    return this;
  }

  public SqrlRelBuilder empty() {
    builder.empty();
    return this;
  }

  public SqrlRelBuilder values(RelDataType rowType, Object... columnValues) {
    builder.values(rowType, columnValues);
    return this;
  }

  public SqrlRelBuilder values(Iterable<? extends List<RexLiteral>> tupleList,
      RelDataType rowType) {
    builder.values(tupleList, rowType);
    return this;
  }

  public SqrlRelBuilder values(RelDataType rowType) {
    builder.values(rowType);
    return this;
  }

  public SqrlRelBuilder limit(int offset, int fetch) {
    builder.limit(offset, fetch);
    return this;
  }

  public SqrlRelBuilder exchange(RelDistribution distribution) {
    builder.exchange(distribution);
    return this;
  }

  public SqrlRelBuilder sortExchange(RelDistribution distribution, RelCollation collation) {
    builder.sortExchange(distribution, collation);
    return this;
  }

  public SqrlRelBuilder sort(int... fields) {
    builder.sort(fields);
    return this;
  }

  public SqrlRelBuilder sort(RexNode... nodes) {
    builder.sort(nodes);
    return this;
  }

  public SqrlRelBuilder sort(Iterable<? extends RexNode> nodes) {
    builder.sort(nodes);
    return this;
  }

  public SqrlRelBuilder sortLimit(int offset, int fetch, RexNode... nodes) {
    builder.sortLimit(offset, fetch, nodes);
    return this;
  }

  public SqrlRelBuilder sort(RelCollation collation) {
    builder.sort(collation);
    return this;
  }

  public SqrlRelBuilder sortLimit(int offset, int fetch, Iterable<? extends RexNode> nodes) {
    builder.sortLimit(offset, fetch, nodes);
    return this;
  }

  public SqrlRelBuilder convert(RelDataType castRowType, boolean rename) {
    builder.convert(castRowType, rename);
    return this;
  }

  public SqrlRelBuilder permute(Mapping mapping) {
    builder.permute(mapping);
    return this;
  }

  public SqrlRelBuilder match(RexNode pattern, boolean strictStart, boolean strictEnd,
      Map<String, RexNode> patternDefinitions, Iterable<? extends RexNode> measureList,
      RexNode after, Map<String, ? extends SortedSet<String>> subsets, boolean allRows,
      Iterable<? extends RexNode> partitionKeys, Iterable<? extends RexNode> orderKeys,
      RexNode interval) {
    builder.match(pattern, strictStart, strictEnd, patternDefinitions, measureList, after,
      subsets, allRows, partitionKeys, orderKeys, interval);
    return this;
  }

  public SqrlRelBuilder pivot(GroupKey groupKey, Iterable<? extends AggCall> aggCalls,
      Iterable<? extends RexNode> axes,
      Iterable<? extends Entry<String, ? extends Iterable<? extends RexNode>>> values) {
    builder.pivot(groupKey, aggCalls, axes, values);
    return this;
  }

  public SqrlRelBuilder hints(RelHint... hints) {
    builder.hints(hints);
    return this;
  }

  public SqrlRelBuilder hints(Iterable<RelHint> hints) {
    builder.hints(hints);
    return this;
  }

  public void clear() {
    builder.clear();
  }

  public SqrlRelBuilder projectAll(boolean hidden) {
    List<RelDataTypeField> fields = builder.peek().getRowType().getFieldList();

    List<RexNode> selects = new ArrayList<>();
    List<String> fieldNames = new ArrayList<>();
    for (RelDataTypeField f : fields) {
      if (!f.getName().startsWith("_")) {
        selects.add(builder.field(f.getName()));
        fieldNames.add(f.getName());
      }
    }

    builder.project(selects, fieldNames);
    return this;
  }


  public SqrlRelBuilder scanSqrl(String name) {
    return scanSqrl(List.of(name));
  }

  public static SqlUserDefinedTableFunction getSqrlTableFunction(QueryPlanner planner, List<String> path) {
    List<SqlOperator> result = new ArrayList<>();
    String tableFunctionName = String.join(".", path);
    //get latest function
    String latestVersionName = getLatestVersion(planner.getSchema().plus().getFunctionNames(), tableFunctionName);
    if (latestVersionName == null) {
      //todo return optional
      return null;
//      throw new RuntimeException("Could not find function: " + path);
    }
    planner.getOperatorTable().lookupOperatorOverloads(new SqlIdentifier(List.of(latestVersionName), SqlParserPos.ZERO),
        SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION, SqlSyntax.FUNCTION, result, planner.getCatalogReader().nameMatcher());

    if (result.size() != 1) {
      throw new RuntimeException("Could not resolve table: " + path);
    }

    return (SqlUserDefinedTableFunction)result.get(0);
  }

  public static String getLatestVersion(Collection<String> functionNames, String prefix) {
    //todo: use name comparator
    Set<String> functionNamesCanon = functionNames.stream().map(f-> Name.system(f).getCanonical())
        .collect(Collectors.toSet());
    String prefixCanon = Name.system(prefix).getCanonical();

    Pattern pattern = Pattern.compile("^"+Pattern.quote(prefixCanon) + "\\$(\\d+)");
    int maxVersion = -1;

    for (String function : functionNamesCanon) {
      Matcher matcher = pattern.matcher(function);
      if (matcher.find()) {
        int version = Integer.parseInt(matcher.group(1));
        maxVersion = Math.max(maxVersion, version);
      }
    }

    return maxVersion != -1 ? prefix + "$" + maxVersion : null;
  }

  public static int getNextVersion(Collection<String> functionNames, String prefix) {
    //todo: use name comparator
    Set<String> functionNamesCanon = functionNames.stream().map(f-> Name.system(f).getCanonical())
        .collect(Collectors.toSet());
    String prefixCanon = Name.system(prefix).getCanonical();

    Pattern pattern = Pattern.compile("^"+Pattern.quote(prefixCanon) + "\\$(\\d+)");
    int maxVersion = -1;

    for (String function : functionNamesCanon) {
      Matcher matcher = pattern.matcher(function);
      if (matcher.find()) {
        int version = Integer.parseInt(matcher.group(1));
        maxVersion = Math.max(maxVersion, version);
      }
    }

    return maxVersion != -1 ? maxVersion + 1 : 0;
  }

  /**
   * Returns a sqrl version of this table
   */
  public SqrlRelBuilder scanSqrl(List<String> names) {
    SqrlPreparingTable relOptTable = this.catalogReader.getSqrlTable(names);
    if (relOptTable == null) {
      throw new RuntimeException("Could not find table: " + names);
    }
    RelDataType shadowed = relOptTable.getRelDataType();

    scan(relOptTable.internalTable.getQualifiedName())
        .project(relOptTable.getInternalTable().getRowType().getFieldList().stream()
            .map(f->field(f.getIndex())).collect(Collectors.toList()), shadowed.getFieldNames(), true);

    return this;
  }

  // SELECT key1, key2, * EXCEPT [key1 key2]
  public SqrlRelBuilder projectAllPrefixDistinct(List<RexNode> firstNodes) {

    List<String> stripped = stripShadowed(builder.peek().getRowType());

    for (RexNode n : firstNodes) {
      if (n instanceof RexInputRef) {
        stripped.remove(builder.peek().getRowType().getFieldNames().get(((RexInputRef) n).getIndex()));
      }
    }

    List<RexNode> project = new ArrayList<>();
    project.addAll(firstNodes);
    for (String field : stripped) {
      RexNode f = builder.field(field);
      project.add(f);
    }

    builder.project(project);
    return this;
  }

  private List<String> stripShadowed(RelDataType fieldList) {
    List<String> names = new ArrayList<>();
    for (RelDataTypeField field : fieldList.getFieldList()) {
      String latest = getLatestVersion(fieldList.getFieldNames(),
          field.getName().split("\\$")[0]);
      if (latest != null
          && !latest.equals(field.getName())) {
        continue;
      }
      names.add(field.getName());
    }

    return names;
  }

  public RexNode evaluateExpression(SqlNode node) {
    return planner.planExpression(node, builder.peek().getRowType(), ReservedName.SELF_IDENTIFIER.getCanonical());
  }

  public RexNode evaluateExpression(SqlNode node, String tableName) {
    return planner.planExpression(node, builder.peek().getRowType(), tableName);
  }

  public List<RexNode> evaluateExpressionsShadowing(List<SqlNode> node, String tableName) {
    List<RexNode> projectExprs = new ArrayList<>();
    RelDataType shadow = builder.peek().getRowType();
    for (SqlNode expr : node) {

      RexNode rexNode = planner.planExpression(expr, shadow, tableName);
      projectExprs.add(rexNode);
    }

    return projectExprs;

  }
  public List<RexNode> evaluateExpressions(List<SqlNode> node, String tableName) {
    List<RexNode> projectExprs = new ArrayList<>();
    for (SqlNode expr : node) {
      projectExprs.add(evaluateExpression(expr, tableName));
    }

    return projectExprs;
  }

  public List<RexNode> evaluateOrderExpression(List<SqlNode> order,
      String tableName) {
    List<RexNode> orderExprs = new ArrayList<>();
    //expression [ ASC | DESC ] [ NULLS FIRST | NULLS LAST ]
    for (SqlNode expr : order) {
      RexNode rex;
      switch (expr.getKind()) {
        case DESCENDING:
        case NULLS_FIRST:
        case NULLS_LAST:
          SqlCall op = (SqlCall) expr;
          rex = evaluateExpression(op.getOperandList().get(0), tableName);
          rex = builder.call(op.getOperator(), rex);
          break;
        default:
          rex = evaluateExpression(expr, tableName);
      }

      orderExprs.add(rex);
    }
    return orderExprs;
  }

  public SqrlRelBuilder distinctOnHint(int numKeys) {
    RelHint hint = RelHint.builder("DISTINCT_ON")
        .hintOptions(IntStream.range(0, numKeys)
            .mapToObj(Integer::toString)
            .collect(Collectors.toList()))
        .build();
    hints(hint);
    return this;
  }

  public SqrlRelBuilder stream(StreamType type) {
    LogicalStream logicalStream = LogicalStream.create(build(), type);
    push(logicalStream);
    return this;
  }

  public SqrlRelBuilder topNHint(int size) {
    RelHint hint = RelHint.builder("TOP_N")
        .hintOptions(IntStream.range(0, size)
            .mapToObj(Integer::toString)
            .collect(Collectors.toList()))
        .build();
    hints(hint);
    return this;
  }

  public SqrlRelBuilder projectAll() {
    project(fields(), peek().getRowType().getFieldNames(), true);
    return this;
  }

  public SqrlRelBuilder projectLast(List<String> firstNames, List<String> names) {
    List<RexNode> first = IntStream.range(0, firstNames.size())
        .mapToObj(i->field(i))
        .collect(Collectors.toList());

    List<RexNode> proj = builder.fields(
        ImmutableBitSet.range(builder.peek().getRowType().getFieldCount() - names.size(),
            builder.peek().getRowType().getFieldCount()));

    project(ListUtils.union(first, proj), ListUtils.union(firstNames, names), true);
    return this;
  }

  public SqrlRelBuilder project(List<String> fieldNames) {
    //append field names to end
    List<RelDataTypeField> fieldList = peek().getRowType().getFieldList();
    int offset = fieldList.size() - fieldNames.size();

    List<RexNode> rexNodes = new ArrayList<>();
    List<String> names = new ArrayList<>();
    for (int i = 0; i < fieldList.size(); i++) {
      rexNodes.add(field(i));
      if (offset < i) {
        names.add(fieldList.get(i).getName());
      } else {
        names.add(fieldNames.get(i - offset));
      }

    }
    project(rexNodes, names, true);
    return this;
  }

  public RelNode buildAndUnshadow() {
    RelNode relNode = build();

    //Before macro expansion, clean up the rel
    relNode = planner.run(relNode,
        CoreRules.FILTER_INTO_JOIN,
        new ExpandTableMacroRule(planner),
        CoreRules.FILTER_INTO_JOIN);

    //Convert lateral joins
    relNode = RelDecorrelator.decorrelateQuery(relNode, planner.getRelBuilder());

    return relNode;
  }

  public SqrlRelBuilder addColumnOp(RexNode rexNode, String name, SqrlPreparingTable toTable) {
    RelNode node = stripProject(build()); //remove sqrl fields from shadowing
    LogicalAddColumnOp op = new LogicalAddColumnOp(planner.getCluster(), null, node, rexNode, name,
        toTable);
    push(op);
    return this;
  }

  public SqrlRelBuilder assignTimestampOp(int index) {
    RelNode node = stripProject(build()); //remove sqrl fields from shadowing
    LogicalAssignTimestampOp op = new LogicalAssignTimestampOp(planner.getCluster(), null,
        node, index);
    push(op);
    return this;
  }

  private RelNode stripProject(RelNode relNode) {
    if (relNode instanceof LogicalProject) {
      return relNode.getInput(0);
    }
    return relNode;
  }

  public SqrlRelBuilder createReferenceOp(List<String> path,
      List<List<String>> tableReferences, SqrlTableFunctionDef def) {
    RelNode relNode = build();

    RelNode op = new LogicalCreateReferenceOp(this.planner.getCluster(), null,
        path, tableReferences, def, relNode);
    push(op);
    return this;
  }

  public SqrlRelBuilder export(List<String> path) {
    RelNode relNode = build();

    LogicalExportOp exportOp = new LogicalExportOp(planner.getCluster(), null, relNode,
        path);
    push(exportOp);
    return this;
  }

  public SqrlRelBuilder createStreamOp(List<String> path, Optional<SqlNodeList> hints,
      SqrlTableFunctionDef sqrlTableFunctionDef, RelOptTable fromRelOptTable) {
    LogicalStream relNode = (LogicalStream)build();

    LogicalCreateStreamOp op = new LogicalCreateStreamOp(planner.getCluster(), null,
        relNode, path, fromRelOptTable, hints, sqrlTableFunctionDef);
    push(op);
    return this;
  }

  public SqrlRelBuilder projectAllResetNames() {
    RelDataType type = builder.peek().getRowType();
    Set<String> names = new LinkedHashSet<>();
    List<RexNode> nodes = new ArrayList<>();
    for (RelDataTypeField field : type.getFieldList()) {
      String n = field.getName().split("\\$")[0];
      if (!names.contains(n)) {
        names.add(n);
        nodes.add(field(field.getIndex()));
      }
    }

    projectNamed(nodes, names, true);
    return this;
  }
}
