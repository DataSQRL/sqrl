package ai.dataeng.sqml.io.sources.formats;

import ai.dataeng.sqml.config.error.ErrorCollector;

import java.io.Serializable;

public interface FormatConfiguration extends Serializable {

    public boolean validate(ErrorCollector errors);

}
