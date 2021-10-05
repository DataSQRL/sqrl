package ai.dataeng.sqml.ingest.schema;

import ai.dataeng.sqml.ingest.DatasetRegistration;
import ai.dataeng.sqml.ingest.source.SourceRecord;
import ai.dataeng.sqml.schema2.basic.ConversionError;
import ai.dataeng.sqml.schema2.name.Name;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SchemaValidationProcess extends ProcessFunction<SourceRecord<String>, SourceRecord<Name>> {

    private final OutputTag<SchemaValidationError> errorTag;
    private final FlexibleDatasetSchema.TableField schema;
    private final SchemaAdjustmentSettings settings;
    private final DatasetRegistration dataset;

    private transient SchemaValidator validator;

    public SchemaValidationProcess(OutputTag<SchemaValidationError> error, FlexibleDatasetSchema.TableField schema,
                                   SchemaAdjustmentSettings settings, DatasetRegistration dataset) {
        this.errorTag = error;
        this.schema = schema;
        this.settings = settings;
        this.dataset = dataset;
        this.validator = new SchemaValidator(schema, settings, dataset);
    }

    @Override
    public void processElement(SourceRecord<String> sourceRecord, Context context, Collector<SourceRecord<Name>> out) {
        ConversionError.Bundle<SchemaConversionError> errors = new ConversionError.Bundle<>();
        SourceRecord<Name> result = validator.verifyAndAdjust(sourceRecord, errors);
        if (errors.isFatal()) {
            context.output(errorTag, SchemaValidationError.of(errors,sourceRecord));
        } else {
            out.collect(result);
        }
    }

}
