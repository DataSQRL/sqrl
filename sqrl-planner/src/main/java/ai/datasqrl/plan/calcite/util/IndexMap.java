package ai.datasqrl.plan.calcite.util;

import com.google.common.base.Preconditions;
import lombok.Value;

import java.util.List;
import java.util.Map;

@FunctionalInterface
public interface IndexMap {

    final IndexMap IDENTITY = idx -> idx;

    int map(int index);

    @Value
    class Pair {
        int source;
        int target;
    }

    static IndexMap of(final Map<Integer,Integer> mapping) {
        return idx -> {
            Integer map = mapping.get(idx);
            Preconditions.checkArgument(map!=null,"Invalid index: %s", map);
            return map;
        };
    }

    static IndexMap inverse(final List<Integer> mappingByPosition) {
        return idx -> {
            int pos = mappingByPosition.indexOf(idx);
            Preconditions.checkArgument(pos>=0,"Invalid index: %d", idx);
            return pos;
        };
    }

    static IndexMap singleton(final int source, final int target) {
        return idx -> {
            Preconditions.checkArgument(idx == source,"Only maps a single source [%d] but given: %d",source,idx);
            return target;
        };
    }

}
