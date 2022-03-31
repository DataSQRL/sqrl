package ai.dataeng.sqml.io.sources.formats;

import ai.dataeng.sqml.config.error.ErrorCollector;
import ai.dataeng.sqml.io.sources.impl.file.FileSourceConfiguration;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.vertx.core.json.Json;

import java.io.Serializable;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({ @JsonSubTypes.Type(value = CSVFormat.Configuration.class, name = CSVFormat.NAME),
        @JsonSubTypes.Type(value = JsonLineFormat.Configuration.class, name = JsonLineFormat.NAME), })
public interface FormatConfiguration extends Serializable {

    boolean validate(ErrorCollector errors);

    @JsonIgnore
    FileFormat getFileFormat();

    @JsonIgnore
    String getName();

}
