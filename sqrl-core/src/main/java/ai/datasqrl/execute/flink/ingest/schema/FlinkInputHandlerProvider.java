package ai.datasqrl.execute.flink.ingest.schema;

import ai.datasqrl.schema.input.FlexibleDatasetSchema;
import ai.datasqrl.schema.input.FlexibleTableConverter;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.Schema;

public class FlinkInputHandlerProvider {

  public FlinkInputHandler  get(FlexibleDatasetSchema.TableField inputTable) {
    FlexibleTableConverter converter = new FlexibleTableConverter(inputTable);
    return new FlinkInputHandler(FlinkTableSchemaGenerator.convert(converter),
            FlinkTypeInfoSchemaGenerator.convert(converter),
            new SourceRecord2RowMapper(inputTable));
  }

}
