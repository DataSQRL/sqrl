package ai.dataeng.sqml.ingest.schema;

import ai.dataeng.sqml.ingest.DatasetRegistration;
import ai.dataeng.sqml.ingest.source.SourceRecord;
import ai.dataeng.sqml.schema2.basic.ConversionError;
import ai.dataeng.sqml.schema2.name.Name;
import lombok.Value;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class SchemaValidationProcess extends ProcessFunction<SourceRecord<String>, SourceRecord<Name>> {

    private final OutputTag<Error> errorTag;
    private final SchemaValidator validator;

    public SchemaValidationProcess(OutputTag<Error> error, FlexibleDatasetSchema.TableField schema,
                                   SchemaAdjustmentSettings settings, DatasetRegistration dataset) {
        this.errorTag = error;
        this.validator = new SchemaValidator(schema, settings, dataset);
    }

    @Override
    public void processElement(SourceRecord<String> sourceRecord, Context context, Collector<SourceRecord<Name>> out) {
        ConversionError.Bundle<SchemaConversionError> errors = new ConversionError.Bundle<>();
        SourceRecord<Name> result = validator.verifyAndAdjust(sourceRecord, errors);
        if (errors.isFatal()) {
            context.output(errorTag, Error.of(errors,sourceRecord));
        } else {
            out.collect(result);
        }
    }

    @Value
    public static class Error implements Serializable {

        private List<String> errors;
        private SourceRecord<String> sourceRecord;

        public static Error of(ConversionError.Bundle<SchemaConversionError> errors, SourceRecord<String> sourceRecord) {
            List<String> errorMsgs = new ArrayList<>();
            errors.forEach(e -> errorMsgs.add(e.toString()));
            return new Error(errorMsgs, sourceRecord);
        }

    }
}
