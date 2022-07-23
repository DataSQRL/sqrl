package ai.datasqrl.plan.calcite.util;

import lombok.Value;

public interface IndexMap {

    int map(int index);

    @Value
    public static class Pair {
        int source;
        int target;
    }

}
