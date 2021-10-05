package ai.dataeng.sqml.ingest.schema;

import ai.dataeng.sqml.ingest.source.SourceRecord;
import ai.dataeng.sqml.schema2.basic.ConversionError;
import lombok.Value;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Value
public class SchemaValidationError implements Serializable {

    private List<String> errors;
    private SourceRecord<String> sourceRecord;

    public static SchemaValidationError of(ConversionError.Bundle<SchemaConversionError> errors, SourceRecord<String> sourceRecord) {
        List<String> errorMsgs = new ArrayList<>();
        errors.forEach(e -> errorMsgs.add(e.toString()));
        return new SchemaValidationError(errorMsgs, sourceRecord);
    }

}
