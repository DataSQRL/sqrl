package ai.datasqrl.plan.calcite.util;

import com.google.common.base.Preconditions;
import lombok.NonNull;
import lombok.Value;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@FunctionalInterface
public interface IndexMap {

    final IndexMap IDENTITY = idx -> idx;

    int map(int index);

    default RexNode map(@NonNull RexNode node) {
        return node.accept(new RexIndexMapShuttle(this));
    }

    default RelCollation map(RelCollation collation) {
        return RelCollations.of(collation.getFieldCollations().stream().map(fc -> fc.withFieldIndex(this.map(fc.getFieldIndex()))).collect(Collectors.toList()));
    }

    @Value
    class RexIndexMapShuttle extends RexShuttle {

        private final IndexMap map;

        @Override public RexNode visitInputRef(RexInputRef input) {
            return new RexInputRef(map.map(input.getIndex()), input.getType());
        }
    }

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
