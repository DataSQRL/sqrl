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
package com.datasqrl.calcite;

import com.datasqrl.calcite.SqrlRexUtil.JoinConditionDecomposition.EqualityCondition;
import com.datasqrl.plan.util.SelectIndexMap;
import com.datasqrl.util.CalciteUtil;
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
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.schema.TableFunction;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Util;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableList;
import org.apache.flink.table.planner.calcite.FlinkRexBuilder;

public class SqrlRexUtil {

  private final RexBuilder rexBuilder;

  public SqrlRexUtil(RelDataTypeFactory typeFactory) {
    rexBuilder = new FlinkRexBuilder(typeFactory);
  }

  public SqrlRexUtil(RelBuilder relB) {
    this(relB.getTypeFactory());
  }

  public SqrlRexUtil(RexBuilder rexBuilder) {
    this(rexBuilder.getTypeFactory());
  }

  public static Optional<Integer> getLimit(RexNode limit) {
    if (limit == null) {
      return Optional.empty();
    } else if (limit instanceof RexLiteral literal) {
      return Optional.of(literal.getValueAs(Integer.class));
    } else {
      return Optional.empty();
    }
  }

  public RexBuilder getBuilder() {
    return rexBuilder;
  }

  public RexNode toDnf(RexNode condition) {
    return RexUtil.toDnf(rexBuilder, condition);
  }

  public List<RexNode> getConjunctions(RexNode condition) {
    //    RexNode cnfCondition = FlinkRexUtil.toCnf(rexBuilder, Short.MAX_VALUE,
    //        condition); //TODO: make configurable
    //    List<RexNode> conditions = new ArrayList<>();
    //    if (cnfCondition instanceof RexCall && cnfCondition.isA(SqlKind.AND)) {
    //      conditions.addAll(((RexCall) cnfCondition).getOperands());
    //    } else { //Single condition
    //      conditions.add(cnfCondition);
    //    }
    return RelOptUtil.conjunctions(condition);
  }

  public List<RexNode> getDisjunctions(RexNode condition) {
    return RelOptUtil.disjunctions(condition);
  }

  public JoinConditionDecomposition decomposeJoinCondition(RexNode condition, int leftSideMaxIdx) {
    var conjunctions = getConjunctions(condition);
    List<EqualityCondition> equalities = new ArrayList<>();
    List<RexNode> remaining = new ArrayList<>();
    for (RexNode rex : conjunctions) {
      var eq = decomposeEqualityCondition(rex, leftSideMaxIdx);
      if (eq.isPresent()) {
        equalities.add(eq.get());
      } else {
        remaining.add(rex);
      }
    }
    return new JoinConditionDecomposition(equalities, remaining);
  }

  private Optional<EqualityCondition> decomposeEqualityCondition(
      RexNode predicate, int leftSideMaxIdx) {
    if (predicate.isA(SqlKind.EQUALS)) {
      var equality = (RexCall) predicate;
      var leftIndex = CalciteUtil.getNonAlteredInputRef(equality.getOperands().get(0));
      var rightIndex = CalciteUtil.getNonAlteredInputRef(equality.getOperands().get(1));
      if (leftIndex.isPresent() && rightIndex.isPresent()) {
        var leftIdx = Math.min(leftIndex.get(), rightIndex.get());
        var rightIdx = Math.max(leftIndex.get(), rightIndex.get());
        if (leftIdx < leftSideMaxIdx && rightIdx >= leftSideMaxIdx) {
          return Optional.of(new EqualityCondition(leftIdx, rightIdx));
        } else {
          return Optional.empty();
        }
      }
      // Check if the constrained side is constrained by an expression that contains entirely of
      // constants of
      // input references from the other side.
      RexNode otherSide;
      int constrainedIdx;
      if (leftIndex.isPresent()) {
        otherSide = equality.getOperands().get(1);
        constrainedIdx = leftIndex.get();
      } else if (rightIndex.isPresent()) {
        otherSide = equality.getOperands().get(0);
        constrainedIdx = rightIndex.get();
      } else {
        return Optional.empty();
      }
      var refs = findAllInputRefs(List.of(otherSide));
      if (constrainedIdx < leftSideMaxIdx && refs.stream().allMatch(idx -> idx >= leftSideMaxIdx)) {
        return Optional.of(new EqualityCondition(constrainedIdx, EqualityCondition.NO_INDEX));
      } else if (constrainedIdx >= leftSideMaxIdx
          && refs.stream().allMatch(idx -> idx < leftSideMaxIdx)) {
        return Optional.of(new EqualityCondition(EqualityCondition.NO_INDEX, constrainedIdx));
      }
    }
    return Optional.empty();
  }

  private Optional<Integer> getInputRefIndex(RexNode node) {
    if (node instanceof RexInputRef ref) {
      return Optional.of(ref.getIndex());
    }
    return Optional.empty();
  }

  @Value
  public static final class JoinConditionDecomposition {

    List<EqualityCondition> equalities;
    List<RexNode> remainingPredicates;

    public List<EqualityCondition> getTwoSidedEqualities() {
      var result = new ArrayList<EqualityCondition>();
      equalities.stream().filter(EqualityCondition::isTwoSided).forEach(result::add);
      return result;
    }

    @Value
    public static final class EqualityCondition {

      public static final int NO_INDEX = -1;

      public int leftIndex;
      public int rightIndex;

      public EqualityCondition(int leftIndex, int rightIndex) {
        Preconditions.checkArgument(leftIndex < rightIndex || rightIndex == NO_INDEX);
        Preconditions.checkArgument(leftIndex >= 0 || rightIndex >= 0);
        this.leftIndex = leftIndex;
        this.rightIndex = rightIndex;
      }

      public boolean isTwoSided() {
        return leftIndex >= 0 && rightIndex >= 0;
      }

      public int getOneSidedIndex() {
        Preconditions.checkArgument(!isTwoSided());
        return (leftIndex >= 0) ? leftIndex : rightIndex;
      }
    }
  }

  public static boolean isNOW(SqlOperator operator) {
    return operator.getName().equalsIgnoreCase("NOW");
  }

  public static RexFinder findFunction(Predicate<SqlOperator> operatorMatch) {
    return new RexFinder<Void>() {
      @Override
      public Void visitCall(RexCall call) {
        if (operatorMatch.test(call.getOperator())) {
          throw Util.FoundOne.NULL;
        }
        return super.visitCall(call);
      }
    };
  }

  public static RexFinder findInputRef(Predicate<RexInputRef> matcher) {
    return new RexFinder<Void>() {
      @Override
      public Void visitInputRef(RexInputRef ref) {
        if (matcher.test(ref)) {
          throw Util.FoundOne.NULL;
        }
        return super.visitInputRef(ref);
      }
    };
  }

  public static RexFinder<RexInputRef> findRexInputRefByIndex(final int index) {
    return new RexFinder<>() {
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
    var inputRefs = findAllInputRefs(call.getOperands());
    if (inputRefs.size() == 1) {
      return Optional.of(Iterables.getOnlyElement(inputRefs));
    }
    return Optional.empty();
  }

  public static Set<Integer> findAllInputRefs(@NonNull Iterable<RexNode> nodes) {
    var refFinder = new RexInputRefFinder();
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
    return IntStream.range(0, size)
        .mapToObj(i -> rexBuilder.makeInputRef(input, i))
        .collect(Collectors.toList());
  }

  public List<RexNode> getProjection(SelectIndexMap select, RelNode input) {
    return select.targetsAsList().stream()
        .map(idx -> rexBuilder.makeInputRef(input, idx))
        .collect(Collectors.toUnmodifiableList());
  }

  public RelBuilder appendColumn(RelBuilder relBuilder, RexNode rexNode, String fieldName) {
    var relNode = relBuilder.peek();
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

  public static List<RexFieldCollation> translateCollation(
      RelCollation collation, RelDataType inputType) {
    return collation.getFieldCollations().stream()
        .map(
            col ->
                new RexFieldCollation(
                    RexInputRef.of(col.getFieldIndex(), inputType), translateOrder(col)))
        .collect(Collectors.toList());
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

  public RexNode createRowFunction(
      SqlAggFunction rowFunction,
      List<RexNode> partition,
      List<RexFieldCollation> fieldCollations) {
    final var intType = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
    var row_function =
        rexBuilder.makeOver(
            intType,
            rowFunction,
            List.of(),
            partition,
            ImmutableList.copyOf(fieldCollations),
            RexWindowBounds.UNBOUNDED_PRECEDING,
            RexWindowBounds.CURRENT_ROW,
            true,
            true,
            false,
            false,
            false);
    return row_function;
  }

  public static List<Integer> combineIndexes(Collection<Integer>... indexLists) {
    List<Integer> result = new ArrayList<>();
    for (Collection<Integer> indexes : indexLists) {
      indexes.stream().filter(Predicate.not(result::contains)).forEach(result::add);
    }
    return result;
  }

  public static RexNode makeWindowLimitFilter(
      RexBuilder rexBuilder, int limit, int fieldIdx, RelDataType windowType) {
    var comparison = SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
    if (limit == 1) {
      comparison = SqlStdOperatorTable.EQUALS;
    }
    return rexBuilder.makeCall(
        comparison,
        RexInputRef.of(fieldIdx, windowType),
        rexBuilder.makeExactLiteral(BigDecimal.valueOf(limit)));
  }

  public static boolean isSimpleProject(LogicalProject project) {
    RexFinder<Void> findComplex =
        new RexFinder<>() {
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

  public RexNode makeInputRef(int colIdx, RelBuilder builder) {
    return rexBuilder.makeInputRef(builder.peek(), colIdx);
  }

  public AggregateCall makeMaxAggCall(int colIdx, String name, int groupCount, RelNode input) {
    return AggregateCall.create(
        SqlStdOperatorTable.MAX,
        false,
        false,
        List.of(colIdx),
        -1,
        RelCollations.EMPTY,
        groupCount,
        input,
        null,
        name);
  }

  public String getFieldName(int idx, RelNode relNode) {
    var rowType = relNode.getRowType();
    return rowType.getFieldList().get(idx).getName();
  }

  public String getCollationName(RelCollation collation, RelNode relNode) {
    return collation.getFieldCollations().stream()
        .map(col -> getFieldName(col.getFieldIndex(), relNode) + " " + col.direction.name())
        .collect(Collectors.joining(","));
  }

  public static Optional<TableFunction> getCustomTableFunction(TableFunctionScan fctScan) {
    var call = (RexCall) fctScan.getCall();
    if (call.getOperator() instanceof SqlUserDefinedTableFunction) {
      return Optional.of(((SqlUserDefinedTableFunction) call.getOperator()).getFunction());
    }
    return Optional.empty();
  }
}
