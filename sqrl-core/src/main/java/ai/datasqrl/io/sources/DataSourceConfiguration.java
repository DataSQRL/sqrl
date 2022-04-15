package ai.datasqrl.io.sources;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.io.SharedConfiguration;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@NoArgsConstructor
public class DataSourceConfiguration extends SharedConfiguration {

    @Override
    protected boolean formatRequired() {
        return false;
    }

    public boolean initialize(ErrorCollector errors) {
        return super.initialize(errors);
    }

}
