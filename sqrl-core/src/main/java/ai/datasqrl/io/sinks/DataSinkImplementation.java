package ai.datasqrl.io.sinks;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.io.impl.file.DirectorySinkImplementation;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "sinkType")
@JsonSubTypes({ @JsonSubTypes.Type(value = DirectorySinkImplementation.class, name = "dir"), })
public interface DataSinkImplementation {

    public boolean initialize(ErrorCollector errors);

}
