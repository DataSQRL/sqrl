/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.calcite.util;

import com.datasqrl.function.SqrlFunction;
import lombok.NonNull;
import lombok.Value;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.IntPair;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableList;
import org.apache.flink.table.api.internal.FlinkEnvProxy;
import org.apache.flink.table.planner.calcite.FlinkRexBuilder;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.planner.plan.utils.FlinkRexUtil;

import java.math.BigDecimal;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SqrlRexUtil {

  private final RexBuilder rexBuilder;

  public SqrlRexUtil(RelDataTypeFactory typeFactory) {
    rexBuilder = new FlinkRexBuilder(typeFactory);
  }

  public SqrlRexUtil(RexBuilder rexBuilder) {
    this(rexBuilder.getTypeFactory());
  }

//
//  public static SqlOperator getSqrlOperator(SqrlFunctionCatalog catalog, String name) {
//    List<SqlOperator> ops = new ArrayList<>();
//    catalog.lookupOperatorOverloads(
//        new SqlIdentifier(name, SqlParserPos.ZERO),
//        SqlFunctionCategory.USER_DEFINED_FUNCTION,
//        SqlSyntax.FUNCTION,
//        ops,
//        SqlNameMatchers.withCaseSensitive(false)
//    );
////
////    for (SqlOperator op : ops) {
////      if (op instanceof BridgingSqlFunction && ((BridgingSqlFunction) op).getDefinition()
////          instanceof SqrlFunction) {
////        return op;
////      }
////    }
//
//    return ops.get(0);
//  }

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
        if (SqrlFunction.unwrapSqrlFunction(call.getOperator()).filter(operatorMatch).isPresent()) {
          throw Util.FoundOne.NULL;
        }
        return super.visitCall(call);
      }
    };
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
        rexBuilder.getTypeFactory().createSqlType(SqlTypeName.INTEGER);
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

  public static Optional<Integer> parseWindowLimitOneFilter(RexNode node) {
    if (node instanceof RexCall) {
      RexCall call = (RexCall) node;
      if (call.getOperator().equals(SqlStdOperatorTable.EQUALS)) {
        if (call.getOperands().get(0) instanceof RexInputRef && call.getOperands()
            .get(1) instanceof RexLiteral) {
          RexLiteral literal = (RexLiteral) call.getOperands().get(1);
          if (literal.getValueAs(BigDecimal.class).equals(BigDecimal.valueOf(1))) {
            return Optional.of(((RexInputRef) call.getOperands().get(0)).getIndex());
          }
        }
      }
    }
    return Optional.empty();
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

  public static boolean hasDeduplicationWindow(RelNode relNode) {
    if (relNode instanceof LogicalProject) {
        if (SqrlRexUtil.isSimpleProject((LogicalProject) relNode)) {
            return hasDeduplicationWindow(relNode.getInput(0));
        }
    }
    if (relNode instanceof LogicalFilter) {
      LogicalFilter filter = (LogicalFilter) relNode;
      Optional<Integer> rowNumIdx = parseWindowLimitOneFilter(filter.getCondition());
      if (rowNumIdx.isPresent()) {
        if (filter.getInput() instanceof LogicalProject) {
          LogicalProject project = (LogicalProject) filter.getInput();
          if (project.getProjects().size() > rowNumIdx.get()) {
            RexFinder<RexOver> findOver = new RexFinder<RexOver>() {
              @Override
              public Void visitOver(RexOver over) {
                throw new Util.FoundOne(over);
              }
            };
            Optional<RexOver> over = findOver.find(project.getProjects().get(rowNumIdx.get()));
            if (over.isPresent()) {
              //TODO: this is a partial check but should be good enough since we only generate over-statements internally
                if (over.get().getOperator() == SqlStdOperatorTable.ROW_NUMBER) {
                    return true;
                } else {
                    return false;
                }
            }
          }
        }
      }
      return hasDeduplicationWindow(filter.getInput());
    }
    return false;
  }

}
