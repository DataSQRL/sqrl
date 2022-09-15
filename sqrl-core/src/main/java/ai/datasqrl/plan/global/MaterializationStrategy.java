package ai.datasqrl.plan.global;

import lombok.AllArgsConstructor;
import lombok.Value;

@Value
@AllArgsConstructor
public class MaterializationStrategy {

    public static final MaterializationStrategy NONE = new MaterializationStrategy(false,null);

    final boolean materialize;
    final String persistedAs;

    public MaterializationStrategy(String persistedAs) {
        this(true,persistedAs);
    }

}
