package ai.datasqrl.plan.calcite.util;

import ai.datasqrl.plan.calcite.SqrlOperatorTable;
import com.google.common.collect.Iterables;
import graphql.com.google.common.base.Preconditions;
import lombok.Value;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang3.tuple.Pair;

import java.math.BigDecimal;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Represents a timestamp predicate that is normalized into the form:
 * smallerIndex <=/= largerIndex + interval
 * The index values refer to timestamp columns in a relation by index or a special timestamp designator.
 */
@Value
public class TimePredicate {

    public static final int NOW_INDEX = -10;

    final int smallerIndex;
    final int largerIndex;
    final boolean smaller;
    final long interval_ms;

    public List<Integer> getIndexes() {
        return List.of(smallerIndex,largerIndex);
    }

    public boolean hasTimestampFunction() {
        return smallerIndex<0 || largerIndex<0;
    }

    public boolean isNowPredicate() {
        return smallerIndex == NOW_INDEX && interval_ms > 0;
    }

    public boolean isUpperBound() {
        return interval_ms<=0;
    }

    public boolean isEquality() {
        return smaller==false;
    }

    public Optional<TimePredicate> and(TimePredicate other) {
        Preconditions.checkArgument(smallerIndex==other.smallerIndex && largerIndex == other.largerIndex,
                "Incompatible indexes: [%s] vs [%s]",this,other);
        if (!smaller && !other.smaller) {
            if (interval_ms == other.interval_ms) return Optional.of(this);
            else return Optional.empty();
        } else if (!smaller) {
            if (interval_ms <= other.interval_ms) return Optional.of(this);
            else return Optional.empty();
        } else if (!other.smaller) {
            if (other.interval_ms <= interval_ms) return Optional.of(other);
            else return Optional.empty();
        } else { //both are inequalities
            return Optional.of(new TimePredicate(smallerIndex,largerIndex,true,Long.min(interval_ms,other.interval_ms)));
        }
    }

    public TimePredicate inverseWithInterval(long interval_ms) {
        return new TimePredicate(largerIndex,smallerIndex,smaller,interval_ms);
    }

    public TimePredicate remap(IndexMap map) {
        return new TimePredicate(smallerIndex<0?smallerIndex:map.map(smallerIndex),
                largerIndex<0?largerIndex:map.map(largerIndex), smaller, interval_ms);
    }

    public RexNode createRexNode(RexBuilder rexBuilder, Function<Integer,RexInputRef> createInputRef) {
        RexNode smallerRef = createRef(smallerIndex, rexBuilder, createInputRef);
        RexNode largerRef = createRef(largerIndex, rexBuilder, createInputRef);
        SqlOperator op = smaller?SqlStdOperatorTable.LESS_THAN_OR_EQUAL:SqlStdOperatorTable.EQUALS;

        if (interval_ms<0) {
            smallerRef = rexBuilder.makeCall(SqlStdOperatorTable.DATETIME_PLUS, smallerRef,
                    makeInterval(interval_ms, rexBuilder));
        } else if (interval_ms > 0) {
            largerRef = rexBuilder.makeCall(SqlStdOperatorTable.DATETIME_PLUS, largerRef,
                    makeInterval(interval_ms, rexBuilder));
        }
        return rexBuilder.makeCall(op,smallerRef,largerRef);
    }

    public static RexNode makeInterval(long interval_ms, RexBuilder rexBuilder) {
        SqlIntervalQualifier sqlIntervalQualifier =
                new SqlIntervalQualifier(TimeUnit.SECOND, TimeUnit.SECOND, SqlParserPos.ZERO);
        return rexBuilder.makeIntervalLiteral(new BigDecimal(interval_ms), sqlIntervalQualifier);
    }

    private static RexNode createRef(int index, RexBuilder rexBuilder,
                                     Function<Integer,RexInputRef> createInputRef) {
        if (index>0) return createInputRef.apply(index);
        else if (index==NOW_INDEX) return rexBuilder.makeCall(SqrlOperatorTable.NOW);
        throw new UnsupportedOperationException("Invalid index: " + index);
    }



    public static final Analyzer ANALYZER = new Analyzer();

    public static class Analyzer {

        public Optional<TimePredicate> extractTimePredicate(RexNode rexNode, RexBuilder rexBuilder,
                                                            Predicate<Integer> isTimestampColumn) {
            if (!(rexNode instanceof RexCall)) return Optional.empty();
            RexCall call = (RexCall) rexNode;
            SqlKind opKind = call.getOperator().getKind();
            switch (opKind) {
                case GREATER_THAN:
                case LESS_THAN:
                case GREATER_THAN_OR_EQUAL:
                case LESS_THAN_OR_EQUAL:
                case EQUALS:
                    break;
                default: return Optional.empty();
            }
            boolean makeEqual = opKind == SqlKind.LESS_THAN || opKind == SqlKind.GREATER_THAN;
            boolean flip = opKind == SqlKind.GREATER_THAN || opKind == SqlKind.GREATER_THAN_OR_EQUAL;
            boolean smaller = opKind != SqlKind.EQUALS;

            Pair<Set<Integer>,RexNode> processLeft = extractReferencesAndReplaceWithZero(call.getOperands().get(0),rexBuilder);
            Pair<Set<Integer>,RexNode> processRight = extractReferencesAndReplaceWithZero(call.getOperands().get(1),rexBuilder);

            //Can only reference a single column or timestamp function on each side
            if (processLeft.getKey().size()!=1 || processRight.getKey().size()!=1) return Optional.empty();
            //Check that the references are timestamp candidates or timestamp function
            int leftRef = Iterables.getOnlyElement(processLeft.getKey()),
                    rightRef = Iterables.getOnlyElement(processRight.getKey());
            for (int ref : new int[]{leftRef, rightRef}) {
                if (ref>0 && !isTimestampColumn.test(ref)) return Optional.empty();
            }
            ExpressionReducer reducer = new ExpressionReducer();
            long[] reduced;
            try {
                reduced = reducer.reduce2Long(rexBuilder, List.of(processLeft.getValue(),processRight.getValue()));
            } catch (IllegalArgumentException exception) {
                return Optional.empty();
            }
            assert reduced.length==2;
            long interval_ms = reduced[1] - reduced[0];

            int smallerIndex = flip?rightRef:leftRef;
            int largerIndex = flip?leftRef:rightRef;
            if (flip) interval_ms *= -1;
            if (makeEqual) interval_ms -=1;

            TimePredicate tp = new TimePredicate(smallerIndex,largerIndex,smaller,interval_ms);
            return Optional.of(tp);
        }

        private Pair<Set<Integer>,RexNode> extractReferencesAndReplaceWithZero(RexNode rexNode, RexBuilder rexBuilder) {
            if (rexNode instanceof RexInputRef) {
                return Pair.of(Set.of(((RexInputRef)rexNode).getIndex()),rexBuilder.makeZeroLiteral(rexNode.getType()));
            }
            if (rexNode instanceof RexCall) {
                RexCall call = (RexCall)rexNode;
                if (call.getOperator().equals(SqrlOperatorTable.NOW)) {
                    return Pair.of(Set.of(NOW_INDEX),rexBuilder.makeZeroLiteral(rexNode.getType()));
                } else {
                    //Map recursively
                    final Set<Integer> allRefs = new HashSet<>();
                    List<RexNode> newOps = call.getOperands().stream().map(op -> {
                        Pair<Set<Integer>,RexNode> result = extractReferencesAndReplaceWithZero(op,rexBuilder);
                        allRefs.addAll(result.getKey());
                        return result.getValue();
                    }).collect(Collectors.toList());
                    return Pair.of(allRefs,rexBuilder.makeCall(call.getType(),call.getOperator(),newOps));
                }
            } else {
                return Pair.of(Set.of(),rexNode);
            }
        }


    }

}
