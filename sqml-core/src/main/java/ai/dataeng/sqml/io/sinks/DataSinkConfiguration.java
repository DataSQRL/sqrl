package ai.dataeng.sqml.io.sinks;

import ai.dataeng.sqml.config.error.ErrorCollector;
import ai.dataeng.sqml.io.impl.file.FileSinkConfiguration;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "sinkType")
@JsonSubTypes({ @JsonSubTypes.Type(value = FileSinkConfiguration.class, name = "file"), })
public interface DataSinkConfiguration {

    public boolean validateAndInitialize(ErrorCollector errors);

}
