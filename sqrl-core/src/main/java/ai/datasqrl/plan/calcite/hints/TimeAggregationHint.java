package ai.datasqrl.plan.calcite.hints;

import com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.rel.hint.RelHint;

@AllArgsConstructor
public class TimeAggregationHint implements SqrlHint {

    public enum Type { TUMBLE, SLIDING }

    @Getter
    final Type type;

    @Override
    public RelHint getHint() {
        return RelHint.builder(CONSTRUCTOR.getName()).hintOption(type.toString()).build();
    }

    public static final Constructor CONSTRUCTOR = new Constructor();

    public static final class Constructor implements SqrlHint.Constructor<TimeAggregationHint> {

        @Override
        public String getName() {
            return TimeAggregationHint.class.getSimpleName();
        }

        @Override
        public TimeAggregationHint fromHint(RelHint hint) {
            Preconditions.checkArgument(hint.listOptions.size()==1,"Invalid hint: %s",hint);
            return new TimeAggregationHint(Type.valueOf(hint.listOptions.get(0)));
        }
    }

}
