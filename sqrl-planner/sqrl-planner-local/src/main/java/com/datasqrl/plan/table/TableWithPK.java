/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.table;

import java.util.List;

public interface TableWithPK {

  String getNameId();

  List<String> getPrimaryKeyNames();
}