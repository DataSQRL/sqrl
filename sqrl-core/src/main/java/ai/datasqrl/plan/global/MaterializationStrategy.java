package ai.datasqrl.plan.global;

import com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.Value;

@Value
@AllArgsConstructor
public class MaterializationStrategy {

    @NonNull
    final MaterializationPreference preference;
    final boolean materialize;
    final String persistedAs;

    public MaterializationStrategy(MaterializationPreference preference, String persistedAs) {
        this(preference,true,persistedAs);
        Preconditions.checkArgument(preference.isMaterialize());
    }

    public MaterializationStrategy(MaterializationPreference preference) {
        this(preference,preference.isMaterialize(), null);
    }


}
