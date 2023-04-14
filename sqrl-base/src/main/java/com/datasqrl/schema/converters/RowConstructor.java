/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema.converters;

import java.io.Serializable;

public interface RowConstructor<R> extends Serializable {

  R createRow(Object[] columns);

  Object createNestedRow(Object[] columns);

  Object createRowList(Object[] rows);

}
