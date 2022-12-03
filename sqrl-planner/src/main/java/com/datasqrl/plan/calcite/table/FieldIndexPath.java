package com.datasqrl.plan.calcite.table;

import com.datasqrl.config.util.AbstractPath;
import lombok.NonNull;
import org.apache.commons.lang3.ArrayUtils;

public final class FieldIndexPath extends AbstractPath<Integer, FieldIndexPath> {

    public static final FieldIndexPath ROOT = new FieldIndexPath();
    private static final Constructor CONSTRUCTOR = new Constructor();

    private FieldIndexPath(Integer... indexes) {
        super(indexes);
    }

    @Override
    protected Constructor constructor() {
        return CONSTRUCTOR;
    }

    public static FieldIndexPath of(int... indexes) {
        return new FieldIndexPath(ArrayUtils.toObject(indexes));
    }


    private static final class Constructor extends AbstractPath.Constructor<Integer, FieldIndexPath> {

        @Override
        protected FieldIndexPath create(@NonNull Integer... elements) {
            return new FieldIndexPath(elements);
        }

        @Override
        protected Integer[] createArray(int length) {
            return new Integer[length];
        }

        @Override
        protected FieldIndexPath root() {
            return ROOT;
        }

    }
}
