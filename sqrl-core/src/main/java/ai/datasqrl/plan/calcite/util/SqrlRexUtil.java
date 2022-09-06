package ai.datasqrl.plan.calcite.util;

import ai.datasqrl.function.SqrlAwareFunction;
import ai.datasqrl.plan.calcite.table.TimestampHolder;
import com.google.common.base.Preconditions;
import lombok.NonNull;
import lombok.Value;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.IntPair;
import org.apache.flink.table.planner.calcite.FlinkRexBuilder;
import org.apache.flink.table.planner.plan.utils.FlinkRexUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SqrlRexUtil {

    private final RexBuilder rexBuilder;

    public SqrlRexUtil(RelDataTypeFactory typeFactory) {
        rexBuilder = new FlinkRexBuilder(typeFactory);
    }

    public RexBuilder getBuilder() {
        return rexBuilder;
    }

    public List<RexNode> getConjunctions(RexNode condition) {
        RexNode cnfCondition = FlinkRexUtil.toCnf(rexBuilder, Short.MAX_VALUE, condition); //TODO: make configurable
        List<RexNode> conditions = new ArrayList<>();
        if (cnfCondition instanceof RexCall && cnfCondition.isA(SqlKind.AND)) {
            conditions.addAll(((RexCall)cnfCondition).getOperands());
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
            if (eq.isPresent()) equalities.add(eq.get());
            else remaining.add(rex);
        }
        return new EqualityComparisonDecomposition(equalities,remaining);
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
            return Optional.of(((RexInputRef)node).getIndex());
        }
        return Optional.empty();
    }

    @Value
    public static final class EqualityComparisonDecomposition {

        List<IntPair> equalities;
        List<RexNode> remainingPredicates;

    }

    public static RexFinder findFunctionByName(final String name) {
        return new RexFinder<Void>() {
            @Override public Void visitCall(RexCall call) {
                if (call.getOperator().getName().equals(name)) {
                    throw Util.FoundOne.NULL;
                }
                return super.visitCall(call);
            }
        };
    }

    public static RexFinder<RexInputRef> findRexInputRefByIndex(final int index) {
        return new RexFinder<RexInputRef>() {
            @Override public Void visitInputRef(RexInputRef ref) {
                if (ref.getIndex()==index) {
                    throw new Util.FoundOne(ref);
                }
                return super.visitInputRef(ref);
            }
        };
    }

    public static RexNode mapIndexes(@NonNull RexNode node, IndexMap map) {
        if (map == null) {
            return node;
        }
        return node.accept(new RexIndexMapShuttle(map));
    }

    @Value
    private static class RexIndexMapShuttle extends RexShuttle {

        private final IndexMap map;

        @Override public RexNode visitInputRef(RexInputRef input) {
            return new RexInputRef(map.map(input.getIndex()), input.getType());
        }
    }

    public List<RexNode> getIdentityProject(RelNode input) {
        return getIdentityProject(input, input.getRowType().getFieldCount());
    }

    public List<RexNode> getIdentityProject(RelNode input, int size) {
        return IntStream.range(0,size).mapToObj(i -> rexBuilder.makeInputRef(input,i)).collect(Collectors.toList());
    }

    public abstract static class RexFinder<R> extends RexVisitorImpl<Void> {
        public RexFinder() {
            super(true);
        }

        public boolean contains(RexNode node) {
            try {
                node.accept(this);
                return false;
            } catch (Util.FoundOne e) {
                return true;
            }
        }

        public Optional<R> find(RexNode node) {
            try {
                node.accept(this);
                return Optional.empty();
            } catch (Util.FoundOne e) {
                return Optional.of((R)e.getNode());
            }
        }
    }

    public Optional<TimestampHolder.Candidate> getPreservedTimestamp(@NonNull RexNode rexNode, @NonNull TimestampHolder.Derived timestamp) {
        if (!(CalciteUtil.isTimestamp(rexNode.getType()))) return Optional.empty();
        if (rexNode instanceof RexInputRef) {
            return timestamp.getCandidateByIndex(((RexInputRef)rexNode).getIndex());
        } else if (rexNode instanceof RexCall) {
            //Determine recursively but ensure there is only one timestamp
            RexCall call = (RexCall) rexNode;
            if (!isTimestampPreservingFunction(call)) return Optional.empty();
            //The above check guarantees that the call only has one timestamp operand which allows us to return the first (if any)
            return call.getOperands().stream().map(param -> getPreservedTimestamp(param, timestamp))
                    .filter(Optional::isPresent).findFirst().orElse(Optional.empty());
        } else {
            return Optional.empty();
        }
    }

    private boolean isTimestampPreservingFunction(RexCall call) {
        SqlOperator operator = call.getOperator();
        if (!(operator instanceof SqrlAwareFunction)) return false; //TODO: generalize to allow other SQL functions
        if (((SqrlAwareFunction)operator).isTimestampPreserving()) {
            //Internal validation that this is a legit timestamp-preserving function
            long numTimeParams = call.getOperands().stream().map(param -> param.getType()).filter(CalciteUtil::isTimestamp).count();
            Preconditions.checkArgument(numTimeParams==1,
                    "%s is an invalid time-preserving function as it allows %d number of timestamp arguments", operator, numTimeParams);
            return true;
        } else return false;
    }

    public Optional<TimeBucketFunctionCall> getTimeBucketingFunction(RexNode rexNode) {
        if (!(rexNode instanceof RexCall)) return Optional.empty();
        RexCall call = (RexCall)rexNode;
        if (!(call.getOperator() instanceof SqrlAwareFunction)) return Optional.empty();
        SqrlAwareFunction bucketFct = (SqrlAwareFunction) call.getOperator();
        if (!bucketFct.isTimeBucketingFunction()) return Optional.empty();
        return Optional.of(TimeBucketFunctionCall.from(call));
    }

}
