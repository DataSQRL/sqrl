package ai.dataeng.sqml.ingest;

import ai.dataeng.sqml.source.SourceRecord;
import lombok.Value;

import java.io.Serializable;

@Value
public class SchemaValidationError implements Serializable {

    private final SchemaAdjustment.ErrorMessage error;
    private final SourceRecord sourceRecord;

}
