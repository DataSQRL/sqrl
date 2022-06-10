package ai.datasqrl.execute.flink.ingest.schema;

import lombok.Value;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.Schema;

@Value
public class FlinkInputHandler {

    private final Schema tableSchema;
    private final TypeInformation typeInformation;
    private final SourceRecord2RowMapper mapper;

}
