package ai.dataeng.sqml.io.sinks;

import ai.dataeng.sqml.config.error.ErrorCollector;
import ai.dataeng.sqml.io.SharedConfiguration;
import lombok.Builder;
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
