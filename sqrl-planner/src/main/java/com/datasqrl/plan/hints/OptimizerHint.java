/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.hints;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.rules.SqrlConverterConfig;
import com.google.common.collect.Iterables;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.sql.SqlHint;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.commons.lang3.tuple.Pair;

public interface OptimizerHint {

  /**
   * Hints that are applied during the logical plan conversion by way of adding them to the
   * {@link SqrlConverterConfig}.
   */
  interface ConverterHint extends OptimizerHint {

    void add2Config(SqrlConverterConfig.SqrlConverterConfigBuilder configBuilder, ErrorCollector errors);

  }

  static Pair<List<OptimizerHint>,List<SqlHint>> fromSqlHints(Optional<SqlNodeList> hints, ErrorCollector errors) {
    List<OptimizerHint> optHints = new ArrayList<>();
    List<SqlHint> otherHints = new ArrayList<>();
    if (hints.isPresent()) {
      for (SqlHint hint : Iterables.filter(hints.get().getList(), SqlHint.class)) {
        String hintname = hint.getName().toLowerCase();
        if (hintname.equalsIgnoreCase(PipelineStageHint.HINT_NAME)) {
          List<String> options = hint.getOptionList();
          errors.checkFatal(options != null && options.size() == 1,
              "Expected a single option for [%s] hint but found: %s", PipelineStageHint.HINT_NAME,
              options);
          optHints.add(new PipelineStageHint(options.get(0).trim()));
        } else if (hintname.equalsIgnoreCase(IndexHint.INDEX_HINT) || hintname.equalsIgnoreCase(
            IndexHint.PARTITION_KEY_HINT)) {
          optHints.add(IndexHint.of(hintname, hint.getOptionList(), errors));
        } else if (hintname.equalsIgnoreCase(PrimaryKeyHint.HINT_NAME)) {
          List<String> options = hint.getOptionList();
          errors.checkFatal(options != null && !options.isEmpty(),
              "%s hint requires at least one column as argument", PrimaryKeyHint.HINT_NAME);
          optHints.add(new PrimaryKeyHint(options));
        } else if (hintname.equalsIgnoreCase(FilteredDistinctOrderHint.HINT_NAME)) {
          optHints.add(new FilteredDistinctOrderHint());
        } else {
          otherHints.add(hint);
        }
      }
    }
    return Pair.of(optHints, otherHints);
  }


}
