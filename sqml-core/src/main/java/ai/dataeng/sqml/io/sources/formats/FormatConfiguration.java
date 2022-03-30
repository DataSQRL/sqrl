package ai.dataeng.sqml.io.sources.formats;

import ai.dataeng.sqml.config.error.ErrorCollector;
import ai.dataeng.sqml.io.sources.impl.file.FileSourceConfiguration;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.io.Serializable;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({ @JsonSubTypes.Type(value = CSVFormat.Configuration.class, name = "csv"),
        @JsonSubTypes.Type(value = JsonLineFormat.Configuration.class, name = "json"), })
public interface FormatConfiguration extends Serializable {

    public boolean validate(ErrorCollector errors);

}
