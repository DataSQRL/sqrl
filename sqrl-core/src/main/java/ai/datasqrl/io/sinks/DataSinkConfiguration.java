package ai.datasqrl.io.sinks;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.io.SharedConfiguration;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@NoArgsConstructor
public class DataSinkConfiguration extends SharedConfiguration {

    @Override
    protected boolean formatRequired() {
        return true;
    }

    public boolean initialize(ErrorCollector errors) {
        return super.initialize(errors);
    }


}
