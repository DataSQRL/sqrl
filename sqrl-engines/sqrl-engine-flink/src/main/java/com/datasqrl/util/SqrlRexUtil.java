/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.util;

import com.datasqrl.function.SqrlFunction;
import com.datasqrl.function.StdTimeLibraryImpl;
import com.datasqrl.plan.hints.DedupHint;
import com.datasqrl.plan.hints.SqrlHint;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.NonNull;
import lombok.Value;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.schema.TableFunction;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.IntPair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableList;
import org.apache.flink.table.catalog.ContextResolvedFunction;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.planner.calcite.FlinkRexBuilder;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.planner.plan.utils.FlinkRexUtil;

public class SqrlRexUtil {

  private final RexBuilder rexBuilder;

  public SqrlRexUtil(RelDataTypeFactory typeFactory) {
    rexBuilder = new FlinkRexBuilder(typeFactory);
  }

  public SqrlRexUtil(RexBuilder rexBuilder) {
    this(rexBuilder.getTypeFactory());
  }

  public RexBuilder getBuilder() {
    return rexBuilder;
  }

  public List<RexNode> getConjunctions(RexNode condition) {
    RexNode cnfCondition = FlinkRexUtil.toCnf(rexBuilder, Short.MAX_VALUE,
        condition); //TODO: make configurable
    List<RexNode> conditions = new ArrayList<>();
    if (cnfCondition instanceof RexCall && cnfCondition.isA(SqlKind.AND)) {
      conditions.addAll(((RexCall) cnfCondition).getOperands());
    } else { //Single condition
      conditions.add(cnfCondition);
    }
    return RelOptUtil.conjunctions(condition);
  }

  public EqualityComparisonDecomposition decomposeEqualityComparison(RexNode condition) {
    List<RexNode> conjunctions = getConjunctions(condition);
    List<IntPair> equalities = new ArrayList<>();
    List<RexNode> remaining = new ArrayList<>();
    for (RexNode rex : conjunctions) {
      Optional<IntPair> eq = getEqualityComparison(rex);
        if (eq.isPresent()) {
            equalities.add(eq.get());
        } else {
            remaining.add(rex);
        }
    }
    return new EqualityComparisonDecomposition(equalities, remaining);
  }

  private Optional<IntPair> getEqualityComparison(RexNode predicate) {
    if (predicate.isA(SqlKind.EQUALS)) {
      RexCall equality = (RexCall) predicate;
      Optional<Integer> leftIndex = getInputRefIndex(equality.getOperands().get(0));
      Optional<Integer> rightIndex = getInputRefIndex(equality.getOperands().get(1));
      if (leftIndex.isPresent() && rightIndex.isPresent()) {
        int leftIdx = Math.min(leftIndex.get(), rightIndex.get());
        int rightIdx = Math.max(leftIndex.get(), rightIndex.get());
        return Optional.of(IntPair.of(leftIdx, rightIdx));
      }
    }
    return Optional.empty();
  }

  private Optional<Integer> getInputRefIndex(RexNode node) {
    if (node instanceof RexInputRef) {
      return Optional.of(((RexInputRef) node).getIndex());
    }
    return Optional.empty();
  }

  @Value
  public static final class EqualityComparisonDecomposition {

    List<IntPair> equalities;
    List<RexNode> remainingPredicates;

  }

  public static RexFinder findFunction(SqrlFunction operator) {
    return findFunction(o -> o.equals(operator));
  }

  public static RexFinder findFunction(Predicate<SqrlFunction> operatorMatch) {
    return new RexFinder<Void>() {
      @Override
      public Void visitCall(RexCall call) {
        if (getSqrlFunction(call.getOperator()).filter(operatorMatch).isPresent()) {
          throw Util.FoundOne.NULL;
        }
        return super.visitCall(call);
      }
    };
  }

  public static Optional<SqrlFunction> getSqrlFunction(SqlOperator operator) {
    if (operator instanceof BridgingSqlFunction) {
      ContextResolvedFunction ctxFunction = ((BridgingSqlFunction)operator).getResolvedFunction();
      FunctionDefinition function = ctxFunction.getDefinition();
      if (function instanceof SqrlFunction) {
        return Optional.of((SqrlFunction) function);
      }
    }
    return Optional.empty();
//    return StdTimeLibraryImpl.lookupSQRLFunction(operator);
  }

  public static RexFinder<RexInputRef> findRexInputRefByIndex(final int index) {
    return new RexFinder<RexInputRef>() {
      @Override
      public Void visitInputRef(RexInputRef ref) {
        if (ref.getIndex() == index) {
          throw new Util.FoundOne(ref);
        }
        return super.visitInputRef(ref);
      }
    };
  }

  public static Optional<Integer> findSingleReferenceColumnIndex(RexCall call) {
    Set<Integer> inputRefs = findAllInputRefs(call.getOperands());
    if (inputRefs.size() == 1) {
      return Optional.of(Iterables.getOnlyElement(inputRefs));
    }
    return Optional.empty();
  }



  public static Set<Integer> findAllInputRefs(@NonNull Iterable<RexNode> nodes) {
    RexInputRefFinder refFinder = new RexInputRefFinder();
      for (RexNode node : nodes) {
          node.accept(refFinder);
      }
    return refFinder.refs;
  }

  @Value
  private static class RexInputRefFinder extends RexShuttle {

    private final Set<Integer> refs = new HashSet<>();

    @Override
    public RexNode visitInputRef(RexInputRef input) {
      refs.add(input.getIndex());
      return input;
    }
  }






  public List<RexNode> getIdentityProject(RelNode input) {
    return getIdentityProject(input, input.getRowType().getFieldCount());
  }

  public List<RexNode> getIdentityProject(RelNode input, int size) {
    return IntStream.range(0, size).mapToObj(i -> rexBuilder.makeInputRef(input, i))
        .collect(Collectors.toList());
  }

  public RelBuilder appendColumn(RelBuilder relBuilder, RexNode rexNode, String fieldName) {
    RelNode relNode = relBuilder.peek();
    List<RexNode> rexes = new ArrayList<>(getIdentityProject(relNode));
    List<String> fieldNames = new ArrayList<>(relNode.getRowType().getFieldNames());
    rexes.add(rexNode);
    fieldNames.add(fieldName);
    relBuilder.projectNamed(rexes, fieldNames, false);
    return relBuilder;
  }

  public abstract static class RexFinder<R> extends RexVisitorImpl<Void> {

    public RexFinder() {
      super(true);
    }

    public boolean foundIn(RexNode node) {
      try {
        node.accept(this);
        return false;
      } catch (Util.FoundOne e) {
        return true;
      }
    }

    public boolean foundIn(Iterable<RexNode> nodes) {
      for (RexNode node : nodes) {
          if (foundIn(node)) {
              return true;
          }
      }
      return false;
    }

    public Optional<R> find(RexNode node) {
      try {
        node.accept(this);
        return Optional.empty();
      } catch (Util.FoundOne e) {
        return Optional.of((R) e.getNode());
      }
    }
  }

  public static List<RexFieldCollation> translateCollation(RelCollation collation,
      RelDataType inputType) {
    return collation.getFieldCollations().stream().map(col -> new RexFieldCollation(
        RexInputRef.of(col.getFieldIndex(), inputType),
        translateOrder(col))).collect(Collectors.toList());
  }

  private static Set<SqlKind> translateOrder(RelFieldCollation collation) {
    Set<SqlKind> result = new HashSet<>();
      if (collation.direction.isDescending()) {
          result.add(SqlKind.DESCENDING);
      }
      if (collation.nullDirection == RelFieldCollation.NullDirection.FIRST) {
          result.add(SqlKind.NULLS_FIRST);
      } else if (collation.nullDirection == RelFieldCollation.NullDirection.LAST) {
          result.add(SqlKind.NULLS_LAST);
      } else {
          result.add(SqlKind.NULLS_LAST);
      }
    return result;
  }

  public RexNode createRowFunction(SqlAggFunction rowFunction, List<RexNode> partition,
      List<RexFieldCollation> fieldCollations) {
    final RelDataType intType =
        rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
    RexNode row_function = rexBuilder.makeOver(intType, rowFunction,
        List.of(), partition, ImmutableList.copyOf(fieldCollations),
        RexWindowBounds.UNBOUNDED_PRECEDING,
        RexWindowBounds.CURRENT_ROW, true, true, false,
        false, false);
    return row_function;
  }

  public static List<Integer> combineIndexes(Collection<Integer>... indexLists) {
    List<Integer> result = new ArrayList<>();
    for (Collection<Integer> indexes : indexLists) {
      indexes.stream().filter(Predicate.not(result::contains)).forEach(result::add);
    }
    return result;
  }

  public static RexNode makeWindowLimitFilter(RexBuilder rexBuilder, int limit, int fieldIdx,
      RelDataType windowType) {
    SqlBinaryOperator comparison = SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
      if (limit == 1) {
          comparison = SqlStdOperatorTable.EQUALS;
      }
    return rexBuilder.makeCall(comparison, RexInputRef.of(fieldIdx, windowType),
        rexBuilder.makeExactLiteral(BigDecimal.valueOf(limit)));
  }

  public static boolean isSimpleProject(LogicalProject project) {
    RexFinder<Void> findComplex = new RexFinder<Void>() {
      @Override
      public Void visitOver(RexOver over) {
        throw Util.FoundOne.NULL;
      }

      @Override
      public Void visitSubQuery(RexSubQuery subQuery) {
        throw Util.FoundOne.NULL;
      }
    };
    return !findComplex.foundIn(project.getProjects());
  }

  public static boolean isDedupedRelNode(RelNode relNode, boolean includeAggregation, boolean allowFilter) {
    if (relNode instanceof LogicalProject) {
      if (SqrlRexUtil.isSimpleProject((LogicalProject) relNode)) {
        return isDedupedRelNode(relNode.getInput(0), includeAggregation, allowFilter);
      }
    }
    Optional<DedupHint> dedupHint = SqrlHint.fromRel(relNode, DedupHint.CONSTRUCTOR);
    if (dedupHint.isPresent()) {
      return true;
    } else if (includeAggregation && (relNode instanceof LogicalAggregate)) {
      return true;
    } else if (allowFilter && (relNode instanceof LogicalFilter)) {
      return isDedupedRelNode(relNode.getInput(0), includeAggregation, allowFilter);
    } else {
      return false;
    }
  }

  public RexNode maxOfTwoColumnsNotNull(int col1Idx, int col2Idx, RelNode input) {
    Preconditions.checkArgument(col1Idx>=0 && col2Idx >= 0 && col1Idx!=col2Idx);
    RexInputRef col1 = rexBuilder.makeInputRef(input, col1Idx);
    RexInputRef col2 = rexBuilder.makeInputRef(input, col2Idx);
    return rexBuilder.makeCall(SqlStdOperatorTable.CASE, rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, col1, col2), col2, col1);
  }

  public RexNode makeInputRef(int colIdx, RelBuilder builder) {
    return rexBuilder.makeInputRef(builder.peek(), colIdx);
  }

  public AggregateCall makeMaxAggCall(int colIdx, String name, int groupCount, RelNode input) {
    return AggregateCall.create(SqlStdOperatorTable.MAX, false, false, false,
        List.of(colIdx) , -1, RelCollations.EMPTY, groupCount, input, null, name);
  }

  public String getFieldName(int idx, RelNode relNode) {
    RelDataType rowType = relNode.getRowType();
    return rowType.getFieldList().get(idx).getName();
  }

  public String getCollationName(RelCollation collation, RelNode relNode) {
    return collation.getFieldCollations().stream()
        .map(col -> getFieldName(col.getFieldIndex(),relNode) + " " + col.direction.name())
        .collect(Collectors.joining(","));
  }

  public static Optional<TableFunction> getCustomTableFunction(TableFunctionScan fctScan) {
    RexCall call = (RexCall) fctScan.getCall();
    if (call.getOperator() instanceof SqlUserDefinedTableFunction) {
      return Optional.of(((SqlUserDefinedTableFunction)call.getOperator()).getFunction());
    }
    return Optional.empty();
  }


}
