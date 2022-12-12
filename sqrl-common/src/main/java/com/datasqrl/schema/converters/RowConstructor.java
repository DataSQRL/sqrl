package com.datasqrl.schema.converters;

import java.io.Serializable;

public interface RowConstructor<R> extends Serializable {

  R createRow(Object[] columns);

  Object createNestedRow(Object[] columns);

  Object createRowList(Object[] rows);

}
