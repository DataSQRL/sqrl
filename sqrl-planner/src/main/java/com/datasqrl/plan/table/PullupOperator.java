/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.table;

import lombok.Value;

/**
 * Database pull-ups are top-level relational operators in table definitions that are more
 * efficiently executed in the database or become irrelevant when the table data is persisted and as
 * such are "pulled up" from the table rel node so they can be pushed into the database if this
 * table happens to be persisted only.
 */
public interface PullupOperator {

  @Value
  final class Container {

    public static final Container EMPTY = new Container(NowFilter.EMPTY, TopNConstraint.EMPTY,
        SortOrder.EMPTY);

    final NowFilter nowFilter;
    final TopNConstraint topN;
    final SortOrder sort;

    public boolean isEmpty() {
      return nowFilter.isEmpty() && topN.isEmpty() && sort.isEmpty();
    }

  }


}
