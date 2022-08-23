package ai.datasqrl.plan.calcite.util;

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
        return new IndexMap() {
            @Override
            public int map(int index) {
                return mapping.get(index);
            }
        };
    }

}
