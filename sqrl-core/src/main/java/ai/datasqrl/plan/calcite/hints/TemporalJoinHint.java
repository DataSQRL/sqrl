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
public class TemporalJoinHint implements SqrlHint {


    final int streamTimestampIdx;
    final int stateTimestampIdx;
    final int[] streamPrimaryKeys;

    @Override
    public RelHint getHint() {
        return RelHint.builder(CONSTRUCTOR.getName()).hintOptions(
                IntStream.concat(IntStream.of(streamTimestampIdx, stateTimestampIdx), IntStream.of(streamPrimaryKeys))
                        .mapToObj(String::valueOf).collect(Collectors.toList())
        ).build();
    }

    public static final Constructor CONSTRUCTOR = new Constructor();

    public static final class Constructor implements SqrlHint.Constructor<TemporalJoinHint> {

        @Override
        public String getName() {
            return TemporalJoinHint.class.getSimpleName();
        }

        @Override
        public TemporalJoinHint fromHint(RelHint hint) {
            List<String> options = hint.listOptions;
            Preconditions.checkArgument(options.size()>=2,"Invalid hint: %s",hint);
            int streamTimeIdx = Integer.valueOf(options.get(0));
            int stateTimeIdx = Integer.valueOf(options.get(1));
            int[] pks = new int[options.size()-2];
            for (int i = 2; i < options.size(); i++) {
                pks[i-2]=Integer.valueOf(options.get(i));
            }
            return new TemporalJoinHint(streamTimeIdx,stateTimeIdx,pks);
        }
    }

}
