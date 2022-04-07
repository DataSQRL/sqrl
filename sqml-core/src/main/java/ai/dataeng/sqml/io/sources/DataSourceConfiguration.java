package ai.dataeng.sqml.io.sources;

import ai.dataeng.sqml.config.error.ErrorCollector;
import ai.dataeng.sqml.io.SharedConfiguration;
import lombok.Builder;
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
