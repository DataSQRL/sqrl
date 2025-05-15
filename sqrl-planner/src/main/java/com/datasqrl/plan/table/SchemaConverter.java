package com.datasqrl.plan.table;

import org.apache.calcite.rel.type.RelDataType;

public interface SchemaConverter<S> {

  S convertSchema(RelDataType dataType);

}
