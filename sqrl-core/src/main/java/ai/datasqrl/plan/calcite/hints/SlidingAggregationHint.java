package ai.datasqrl.plan.calcite.hints;

import com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.rel.hint.RelHint;

import java.util.List;

@AllArgsConstructor
@Getter
public class SlidingAggregationHint implements SqrlHint {

    final int timestampIdx;

    final long intervalWidthMs;
    final long slideWidthMs;

    @Override
    public RelHint getHint() {
        return RelHint.builder(CONSTRUCTOR.getName()).hintOptions(List.of(String.valueOf(timestampIdx),
                String.valueOf(intervalWidthMs),String.valueOf(slideWidthMs))).build();
    }

    public static final Constructor CONSTRUCTOR = new Constructor();

    public static final class Constructor implements SqrlHint.Constructor<SlidingAggregationHint> {

        @Override
        public String getName() {
            return SlidingAggregationHint.class.getSimpleName();
        }

        @Override
        public SlidingAggregationHint fromHint(RelHint hint) {
            List<String> options = hint.listOptions;
            Preconditions.checkArgument(hint.listOptions.size()==3,"Invalid hint: %s",hint);
            return new SlidingAggregationHint(Integer.valueOf(options.get(0)),
                    Long.valueOf(options.get(1)),Long.valueOf(options.get(2)));
        }
    }

}
