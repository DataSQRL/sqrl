package ai.datasqrl.plan.calcite.util;

import com.google.common.base.Preconditions;
import lombok.Value;

import java.util.Map;

@FunctionalInterface
public interface IndexMap {

    int map(int index);

    @Value
    class Pair {
        int source;
        int target;
    }

    static IndexMap of(final Map<Integer,Integer> mapping) {
        return idx -> mapping.get(idx);
    }

    static IndexMap singleton(final int source, final int target) {
        return idx -> {
            Preconditions.checkArgument(idx == source,"Only maps a single source [%d] but given: %d",source,idx);
            return target;
        };
    }

}
