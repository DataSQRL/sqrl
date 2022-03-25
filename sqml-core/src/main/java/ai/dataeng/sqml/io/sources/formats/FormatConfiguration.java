package ai.dataeng.sqml.io.sources.formats;

import ai.dataeng.sqml.config.ConfigurationError;
import ai.dataeng.sqml.type.basic.ProcessMessage;

import java.io.Serializable;

public interface FormatConfiguration extends Serializable {

    public boolean validate(ProcessMessage.ProcessBundle<ConfigurationError> errors);

}
