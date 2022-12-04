/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema;

import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

@Getter
public class UnionSQRLTable extends SQRLTable {

  private final List<SQRLTable> tables = new ArrayList<>();

}
