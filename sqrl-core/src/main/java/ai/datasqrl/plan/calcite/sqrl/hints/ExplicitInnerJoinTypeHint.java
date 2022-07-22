package ai.datasqrl.plan.calcite.sqrl.hints;

import com.google.common.base.Preconditions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;

import java.util.List;
import java.util.Optional;

public enum ExplicitInnerJoinTypeHint implements SqrlHint {

    INNER, TEMPORAL, INTERVAL;

    public static final String TYPE_KEY = "type";

    @Override
    public RelHint getHint() {
        return RelHint.builder(CONSTRUCTOR.getName()).hintOption(TYPE_KEY,name()).build();
    }

    public static final Constructor CONSTRUCTOR = new Constructor();

    public static final class Constructor implements SqrlHint.Constructor<ExplicitInnerJoinTypeHint> {

        private Constructor() {}

        @Override
        public String getName() {
            return ExplicitInnerJoinTypeHint.class.getSimpleName();
        }

        @Override
        public ExplicitInnerJoinTypeHint fromHint(RelHint hint) {
            Preconditions.checkArgument(hint.kvOptions.containsKey(TYPE_KEY),"Invalid hint: %s",hint);
            return valueOf(hint.kvOptions.get(TYPE_KEY));
        }
    }








}
