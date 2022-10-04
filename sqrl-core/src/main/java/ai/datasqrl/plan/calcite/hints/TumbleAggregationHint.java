package ai.datasqrl.plan.calcite.hints;

import com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.rel.hint.RelHint;

import java.util.List;

@AllArgsConstructor
public class TumbleAggregationHint implements SqrlHint {

    @Getter
    final int timestampIdx;

    @Override
    public RelHint getHint() {
        return RelHint.builder(CONSTRUCTOR.getName()).hintOptions(List.of(String.valueOf(timestampIdx))).build();
    }

    public static final Constructor CONSTRUCTOR = new Constructor();

    public static final class Constructor implements SqrlHint.Constructor<TumbleAggregationHint> {

        @Override
        public String getName() {
            return TumbleAggregationHint.class.getSimpleName();
        }

        @Override
        public TumbleAggregationHint fromHint(RelHint hint) {
            Preconditions.checkArgument(hint.listOptions.size()==1,"Invalid hint: %s",hint);
            return new TumbleAggregationHint(Integer.valueOf(hint.listOptions.get(0)));
        }
    }

}
