package com.datasqrl.plan.calcite.rules;

import com.datasqrl.physical.ExecutionEngine;
import com.datasqrl.plan.calcite.hints.JoinCostHint;
import com.datasqrl.plan.calcite.hints.SqrlHint;
import com.datasqrl.plan.calcite.table.TableType;
import com.google.common.base.Preconditions;
import javax.validation.constraints.NotNull;
import lombok.Value;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Join;

import java.util.Optional;

@Value
public
class SimpleCostModel implements ComputeCost {

    private final double cost;

    private SimpleCostModel(double cost) {
        this.cost = cost;
    }

    public static SimpleCostModel of(ExecutionEngine.Type engineType, AnnotatedLP annotatedLP) {
        double cost = 1.0;
        if (engineType.isRead()) {
            //Currently we make the simplifying assumption that read execution is the baseline and we compare
            //the stream cost against it
        } else if (engineType.isWrite()) {
            Preconditions.checkArgument(engineType== ExecutionEngine.Type.STREAM);
            //We assume that pre-computing is generally cheaper (by factor of 10) unless (standard) joins are
            //involved which can lead to combinatorial explosion. So, we primarily cost the joins
            cost = joinCost(annotatedLP.getRelNode());
            cost = cost / 10;
        } else throw new UnsupportedOperationException("Unsupported engine type: " + engineType);
        return new SimpleCostModel(cost);
    }

    @Override
    public int compareTo(@NotNull ComputeCost o) {
        Preconditions.checkArgument(o instanceof SimpleCostModel);
        return Double.compare(cost, ((SimpleCostModel) o).cost);
    }

    public static double joinCost(RelNode rootRel) {
        /** Visitor that counts join nodes. */
        class JoinCounter extends RelVisitor {
            double joinCost = 1.0;

            @Override public void visit(RelNode node, int ordinal, RelNode parent) {
                if (node instanceof Join) {
                    Optional<JoinCostHint> costHintOpt = SqrlHint.fromRel(node,JoinCostHint.CONSTRUCTOR);
                    if (costHintOpt.isPresent()) {
                        double localCost = 0.0;
                        JoinCostHint jch = costHintOpt.get();
                        localCost += perSideCost(jch.getLeftType());
                        localCost += perSideCost(jch.getRightType());
                        if (jch.getNumEqualities()==0) localCost *= 100;
                        assert localCost >= 1;
                        joinCost *= localCost;
                    }
                }
                super.visit(node, ordinal, parent);
            }

            private double perSideCost(TableType tableType) {
                switch (tableType) {
                    case STREAM: return 100;
                    case STATE: return 10;
                    case TEMPORAL_STATE: return 4;
                    default: throw new UnsupportedOperationException();
                }
            }

            double run(RelNode node) {
                go(node);
                return joinCost;
            }
        }

        return new JoinCounter().run(rootRel);
    }
}
