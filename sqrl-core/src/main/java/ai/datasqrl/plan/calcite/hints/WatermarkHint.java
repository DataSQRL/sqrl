package ai.datasqrl.plan.calcite.hints;

import com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.rel.hint.RelHint;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Getter
@AllArgsConstructor
public class WatermarkHint implements SqrlHint {

    final int timestampIdx;

    @Override
    public RelHint getHint() {
        return RelHint.builder(CONSTRUCTOR.getName()).hintOptions(
               IntStream.of(timestampIdx)
                        .mapToObj(String::valueOf).collect(Collectors.toList())
        ).build();
    }

    public static final Constructor CONSTRUCTOR = new Constructor();

    public static final class Constructor implements SqrlHint.Constructor<WatermarkHint> {

        @Override
        public String getName() {
            return WatermarkHint.class.getSimpleName();
        }

        @Override
        public WatermarkHint fromHint(RelHint hint) {
            List<String> options = hint.listOptions;
            Preconditions.checkArgument(options.size()==1,"Invalid hint: %s",hint);
            int timestampIdx = Integer.valueOf(options.get(0));
            return new WatermarkHint(timestampIdx);
        }
    }

}
