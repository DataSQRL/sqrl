package ai.datasqrl.execute.flink.ingest;

import ai.datasqrl.io.sources.SourceRecord;
import ai.datasqrl.io.sources.dataset.SourceDataset;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.schema.type.schema.FlexibleDatasetSchema;
import ai.datasqrl.schema.type.schema.SchemaAdjustmentSettings;
import ai.datasqrl.schema.type.schema.SchemaValidator;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import lombok.Value;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SchemaValidationProcess extends ProcessFunction<SourceRecord.Raw, SourceRecord.Named> {

    private final OutputTag<Error> errorTag;
    private final SchemaValidator validator;

    public SchemaValidationProcess(OutputTag<Error> error, FlexibleDatasetSchema.TableField schema,
                                   SchemaAdjustmentSettings settings, SourceDataset.Digest dataset) {
        this.errorTag = error;
        this.validator = new SchemaValidator(schema, settings, dataset);
    }

    @Override
    public void processElement(SourceRecord.Raw sourceRecord, Context context, Collector<SourceRecord.Named> out) {
        ErrorCollector errors = ErrorCollector.root();
        SourceRecord.Named result = validator.verifyAndAdjust(sourceRecord, errors);
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

        public static Error of(ErrorCollector errors, SourceRecord<String> sourceRecord) {
            List<String> errorMsgs = new ArrayList<>();
            errors.forEach(e -> errorMsgs.add(e.toString()));
            return new Error(errorMsgs, sourceRecord);
        }

    }
}
