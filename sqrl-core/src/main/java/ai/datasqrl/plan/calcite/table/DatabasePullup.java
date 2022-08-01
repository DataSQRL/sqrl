package ai.datasqrl.plan.calcite.table;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.calcite.rel.RelNode;

import java.util.List;

/**
 * Database pull-ups are top-level relational operators in table definitions that are more efficiently executed
 * in the database or become irrelevant when the table data is persisted and as such are "pulled up" from the table
 * rel node so they can be pushed into the database if this table happens to be persisted only.
 */
public interface DatabasePullup {


    @AllArgsConstructor
    @Getter
    class Container {

        public static final Container EMPTY = new Container(null,List.of(),false);

        private RelNode baseRelnode;
        private @NonNull List<DatabasePullup> pullups;
        @Setter private boolean inlined;

        public boolean isEmpty() {
            return pullups.isEmpty();
        }

        public void setOptimizedRelNode(RelNode optimized) {
            this.baseRelnode = optimized;
        }



    }



}
