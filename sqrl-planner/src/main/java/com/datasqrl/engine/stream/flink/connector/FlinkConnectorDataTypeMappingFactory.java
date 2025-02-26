package com.datasqrl.engine.stream.flink.connector;

import com.datasqrl.config.TableConfig;
import com.datasqrl.datatype.DataTypeMapper;
import com.datasqrl.datatype.flink.FlinkDataTypeMapper;
import com.datasqrl.util.ServiceLoaderDiscovery;
import java.util.List;
import java.util.Optional;

public class FlinkConnectorDataTypeMappingFactory {
  public Optional<DataTypeMapper> getConnectorMapping(TableConfig tableConfig) {
    List<DataTypeMapper> dataTypeMapperList = ServiceLoaderDiscovery.getAll(DataTypeMapper.class);

    return dataTypeMapperList.stream()
        .filter(m -> m instanceof FlinkDataTypeMapper)
        .filter((m) -> ((FlinkDataTypeMapper) m).isTypeOf(tableConfig))
        .findFirst();
  }
}
