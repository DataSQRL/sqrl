package ai.dataeng.sqml.ingest.schema;

import ai.dataeng.sqml.ingest.source.SourceRecord;
import lombok.Value;

import java.io.Serializable;

@Value
public class SchemaValidationError implements Serializable {

    private final SchemaAdjustment.ErrorMessage error;
    private final SourceRecord sourceRecord;

}
