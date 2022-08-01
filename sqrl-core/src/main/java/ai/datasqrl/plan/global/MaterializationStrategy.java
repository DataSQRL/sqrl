package ai.datasqrl.plan.global;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@Setter
@Getter
public class MaterializationStrategy {

    public boolean materialize = true;
    public String persistedAs = null;
    public boolean pullup = false;

}
