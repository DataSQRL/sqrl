package ai.dataeng.sqml.config.engines;

import javax.annotation.Nullable;

public class FlinkConfiguration implements EngineConfiguration.Stream {

    @Nullable
    public Boolean savepoint;

}
