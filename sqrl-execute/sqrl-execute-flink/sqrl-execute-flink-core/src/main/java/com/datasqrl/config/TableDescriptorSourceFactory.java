package com.datasqrl.config;

import java.util.Optional;
import org.apache.flink.table.api.TableDescriptor;

public interface TableDescriptorSourceFactory extends FlinkSourceFactory<TableDescriptor.Builder> {

  Optional<String> getSourceTimeMetaData();

}
