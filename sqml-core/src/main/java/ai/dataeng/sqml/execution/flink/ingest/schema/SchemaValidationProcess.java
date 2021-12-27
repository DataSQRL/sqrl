package ai.dataeng.sqml.execution.flink.ingest.schema;

import ai.dataeng.sqml.execution.flink.ingest.DatasetRegistration;
import ai.dataeng.sqml.execution.flink.ingest.source.SourceRecord;
import ai.dataeng.sqml.type.basic.ProcessMessage.ProcessBundle;
import ai.dataeng.sqml.tree.name.Name;
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
        ProcessBundle<SchemaConversionError> errors = new ProcessBundle<>();
        SourceRecord<Name> result = validator.verifyAndAdjust(sourceRecord, errors);
        if (errors.isFatal()) {
            context.output(errorTag, SchemaValidationProcess.Error.of(errors,sourceRecord));
        } else {
            out.collect(result);
        }
    }

    @Value
    public static class Error implements Serializable {

        private List<String> errors;
        private SourceRecord<String> sourceRecord;

        public static Error of(ProcessBundle<SchemaConversionError> errors, SourceRecord<String> sourceRecord) {
            List<String> errorMsgs = new ArrayList<>();
            errors.forEach(e -> errorMsgs.add(e.toString()));
            return new Error(errorMsgs, sourceRecord);
        }

    }
}
