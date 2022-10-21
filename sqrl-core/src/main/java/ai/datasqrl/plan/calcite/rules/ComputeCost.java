package ai.datasqrl.plan.calcite.rules;

import ai.datasqrl.physical.ExecutionEngine;
import com.google.common.base.Preconditions;
import lombok.Value;
import org.jetbrains.annotations.NotNull;

public interface ComputeCost extends Comparable<ComputeCost> {

    @Value
    class Simple implements ComputeCost {

        private final double cost;

        private Simple(double cost) {
            this.cost = cost;
        }

        public static Simple of(ExecutionEngine.Type engineType, boolean isExpensive) {
            double cost = isExpensive?100:1;
            if (engineType.isWrite()) cost = cost/10;
            return new Simple(cost);
        }

        @Override
        public int compareTo(@NotNull ComputeCost o) {
            Preconditions.checkArgument(o instanceof Simple);
            return Double.compare(cost,((Simple) o).cost);
        }
    }
}
