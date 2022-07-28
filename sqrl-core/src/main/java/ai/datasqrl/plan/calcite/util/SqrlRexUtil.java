package ai.datasqrl.plan.calcite.util;

import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.Value;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.IntPair;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@AllArgsConstructor
public class SqrlRexUtil {

    private final RexBuilder rexBuilder;

    public List<RexNode> getConjunctions(RexNode condition) {
        //condition = FlinkRexUtil.toCnf(rexBuilder,1000, condition); TODO: add back in and make configurable
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
                return Optional.of(IntPair.of(leftIndex.get(), rightIndex.get()));
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
        return new RexFinder() {
            @Override public Void visitCall(RexCall call) {
                if (call.getOperator().getName().equals(name)) {
                    throw Util.FoundOne.NULL;
                }
                return super.visitCall(call);
            }
        };
    }

    public static RexNode mapIndexes(@NonNull RexNode node, IndexMap map) {
        if (map == null) {
            return node;
        }
        return node.accept(new RexIndexMapShuttle(map));
    }

    private static class RexIndexMapShuttle extends RexShuttle {

        private final IndexMap map;

        private RexIndexMapShuttle(IndexMap map) {
            this.map = map;
        }

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

    public abstract static class RexFinder extends RexVisitorImpl<Void> {
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
    }


}
