package ai.dataeng.sqml.ingest.schema;

import ai.dataeng.sqml.ingest.source.SourceRecord;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SchemaValidationProcess extends ProcessFunction<SourceRecord, SourceRecord> {

    private final OutputTag<SchemaValidationError> errorTag;
    private final SourceTableSchema schema;
    private final SchemaAdjustmentSettings settings;

    public SchemaValidationProcess(OutputTag<SchemaValidationError> error, SourceTableSchema schema, SchemaAdjustmentSettings settings) {
        this.errorTag = error;
        this.schema = schema;
        this.settings = settings;
    }

    @Override
    public void processElement(SourceRecord sourceRecord, Context context, Collector<SourceRecord> out) {
        SchemaAdjustment<SourceRecord> result = schema.verifyAndAdjust(sourceRecord, settings);
        if (result.isError()) {
            context.output(errorTag, new SchemaValidationError(result.getError(),sourceRecord));
        } else {
            if (result.transformedData()) {
                sourceRecord = result.getData();
            }
            out.collect(sourceRecord);
        }
    }

}
