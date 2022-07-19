package ai.datasqrl.plan.calcite.sqrl.table;

import ai.datasqrl.config.AbstractPath;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import lombok.NonNull;
import org.apache.commons.lang3.ArrayUtils;

public final class FieldPath extends AbstractPath<String, FieldPath> {

    public static final FieldPath ROOT = new FieldPath();
    private static final Constructor CONSTRUCTOR = new Constructor();

    private FieldPath(String... indexes) {
        super(indexes);
    }

    @Override
    protected Constructor constructor() {
        return CONSTRUCTOR;
    }

    private static final class Constructor extends AbstractPath.Constructor<String, FieldPath> {

        @Override
        protected FieldPath create(@NonNull String... elements) {
            return new FieldPath(elements);
        }

        @Override
        protected String[] createArray(int length) {
            return new String[length];
        }

        @Override
        protected FieldPath root() {
            return ROOT;
        }

    }
}
