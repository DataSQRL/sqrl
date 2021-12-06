package ai.dataeng.sqml.logical4;

import ai.dataeng.sqml.execution.importer.ImportManager;
import ai.dataeng.sqml.ingest.schema.FlexibleDatasetSchema;
import ai.dataeng.sqml.ingest.schema.SchemaAdjustmentSettings;
import ai.dataeng.sqml.ingest.schema.SchemaConversionError;
import ai.dataeng.sqml.ingest.source.SourceTable;
import ai.dataeng.sqml.schema2.basic.ConversionError;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import lombok.AllArgsConstructor;
import lombok.NonNull;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@AllArgsConstructor
public class SourceOperator extends LogicalPlan.DocumentNode {

    @NonNull
    final FlexibleDatasetSchema.TableField sourceSchema;
    @NonNull
    final SourceTable table;
    @NonNull
    final Map<NamePath, LogicalPlan.Column[]> outputSchema;

    //TODO: Users should be able to control this via global config or hints
    @NonNull
    final SchemaAdjustmentSettings settings = SchemaAdjustmentSettings.DEFAULT;

    @Override
    List<LogicalPlan.DocumentNode> getInputs() {
        return Collections.EMPTY_LIST;
    }

    @Override
    Map<NamePath, LogicalPlan.Column[]> getOutputSchema() {
        return outputSchema;
    }




}
