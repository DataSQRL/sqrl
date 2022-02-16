package ai.dataeng.sqml.planner.operator;

import ai.dataeng.sqml.execution.flink.ingest.schema.FlexibleDatasetSchema;
import ai.dataeng.sqml.execution.flink.ingest.schema.SchemaAdjustmentSettings;
import ai.dataeng.sqml.io.sources.dataset.SourceTable;
import ai.dataeng.sqml.planner.Column;
import ai.dataeng.sqml.planner.LogicalPlanImpl;
import ai.dataeng.sqml.tree.name.NamePath;
import lombok.NonNull;
import lombok.Value;

import java.util.Collections;
import java.util.Map;

/**
 * Represents an event source of document records.
 */
@Value
public class DocumentSource extends LogicalPlanImpl.DocumentNode<LogicalPlanImpl.Node> {

    @NonNull
    final FlexibleDatasetSchema.TableField sourceSchema;
    @NonNull
    final SourceTable table;
    @NonNull
    final Map<NamePath, Column[]> outputSchema;

    //TODO: Users should be able to control this via global config or hints
    @NonNull
    final SchemaAdjustmentSettings settings = SchemaAdjustmentSettings.DEFAULT;

    public DocumentSource(FlexibleDatasetSchema.TableField sourceSchema, @NonNull SourceTable table, @NonNull Map<NamePath, Column[]> outputSchema) {
        super(Collections.EMPTY_LIST);
        this.sourceSchema = sourceSchema;
        this.table = table;
        this.outputSchema = outputSchema;
    }

    @Override
    public Map<NamePath, Column[]> getOutputSchema() {
        return outputSchema;
    }




}
