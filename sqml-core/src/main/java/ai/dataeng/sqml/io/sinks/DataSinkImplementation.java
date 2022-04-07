package ai.dataeng.sqml.io.sinks;

import ai.dataeng.sqml.config.error.ErrorCollector;
import ai.dataeng.sqml.io.impl.file.DirectorySinkImplementation;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "sinkType")
@JsonSubTypes({ @JsonSubTypes.Type(value = DirectorySinkImplementation.class, name = "dir"), })
public interface DataSinkImplementation {

    public boolean initialize(ErrorCollector errors);

}
