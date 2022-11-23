package ai.datasqrl.plan.calcite.hints;

import com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.rel.hint.RelHint;

import java.util.List;

@AllArgsConstructor
public class TumbleAggregationHint implements SqrlHint {

    public enum Type { FUNCTION, INSTANT }

    @Getter
    final int timestampIdx;
    @Getter
    final Type type;

    @Override
    public RelHint getHint() {
        return RelHint.builder(getHintName()).hintOptions(List.of(String.valueOf(timestampIdx), String.valueOf(type))).build();
    }

    public static final String HINT_NAME = TumbleAggregationHint.class.getSimpleName();

    @Override
    public String getHintName() {
        return HINT_NAME;
    }

    public static final Constructor CONSTRUCTOR = new Constructor();

    public static final class Constructor implements SqrlHint.Constructor<TumbleAggregationHint> {

        @Override
        public boolean validName(String name) {
            return name.equalsIgnoreCase(HINT_NAME);
        }

        @Override
        public TumbleAggregationHint fromHint(RelHint hint) {
            Preconditions.checkArgument(hint.listOptions.size()==2,"Invalid hint: %s",hint);
            return new TumbleAggregationHint(Integer.valueOf(hint.listOptions.get(0)),Type.valueOf(hint.listOptions.get(1)));
        }
    }

}
