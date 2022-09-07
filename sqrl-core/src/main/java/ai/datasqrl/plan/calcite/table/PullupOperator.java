package ai.datasqrl.plan.calcite.table;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayDeque;
import java.util.Optional;

/**
 * Database pull-ups are top-level relational operators in table definitions that are more efficiently executed
 * in the database or become irrelevant when the table data is persisted and as such are "pulled up" from the table
 * rel node so they can be pushed into the database if this table happens to be persisted only.
 */
public interface PullupOperator {

    final class Container extends ArrayDeque<Holder> {

        public static final Container EMPTY = new Container();

        public<R extends PullupOperator> Optional<R> getLast(Class<R> clazz) {
            if (!isEmpty()) {
                PullupOperator po = getLast().getPullup();
                if (clazz.isInstance(po)) return Optional.of((R) po);
            }
            return Optional.empty();
        }

    }

    @AllArgsConstructor
    @Getter
    class Holder {

        @NonNull
        private final PullupOperator pullup;
        @NonNull
        private RelNode baseRelNode;

        public void setOptimizedRelNode(RelNode optimized) {
            this.baseRelNode = optimized;
        }

    }



}
