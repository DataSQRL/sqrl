package ai.datasqrl.plan.calcite.hints;

import com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.rel.hint.RelHint;

@AllArgsConstructor
public class NumColumnsHint implements SqrlHint {

    @Getter
    final int numColumns;

    @Override
    public RelHint getHint() {
        return RelHint.builder(CONSTRUCTOR.getName()).hintOption(Integer.toString(numColumns)).build();
    }

    public static final Constructor CONSTRUCTOR = new Constructor();

    public static final class Constructor implements SqrlHint.Constructor<NumColumnsHint> {

        @Override
        public String getName() {
            return NumColumnsHint.class.getSimpleName();
        }

        @Override
        public NumColumnsHint fromHint(RelHint hint) {
            Preconditions.checkArgument(hint.listOptions.size()==1,"Invalid hint: %s",hint);
            return new NumColumnsHint(Integer.parseInt(hint.listOptions.get(0)));
        }
    }

}
