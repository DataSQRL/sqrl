/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.plan.util;

import com.datasqrl.util.CalciteHacks;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.Pair;

/** Methods copied from Calcite's {@link RelWriterImpl} to add hints to the output */
public class RelWriterWithHints extends RelWriterImpl {

  public boolean withHints = true;

  public RelWriterWithHints(PrintWriter pw) {
    super(pw, SqlExplainLevel.EXPPLAN_ATTRIBUTES, false);
  }

  public static String explain(RelNode rel) {
    var sw = new StringWriter();
    var writer = new RelWriterWithHints(new PrintWriter(sw));
    rel.explain(writer);
    return sw.toString();
  }

  /**
   * Copied verbatim from {@link RelWriterImpl} and added the marked section of code to print hints
   *
   * @param rel
   * @param values
   */
  @Override
  protected void explain_(RelNode rel, List<Pair<String, Object>> values) {
    var inputs = rel.getInputs();
    final var mq = rel.getCluster().getMetadataQuery();
    CalciteHacks.resetToSqrlMetadataProvider();

    // removed b/c of
    //    if (!mq.isVisibleInExplain(rel, detailLevel)) {
    // render children in place of this, at same level
    //      explainInputs(inputs);
    //      return;
    //    }

    var s = new StringBuilder();
    spacer.spaces(s);
    if (withIdPrefix) {
      s.append(rel.getId()).append(":");
    }
    s.append(rel.getRelTypeName());
    if (detailLevel != SqlExplainLevel.NO_ATTRIBUTES) {
      var j = 0;
      for (Pair<String, Object> value : values) {
        if (value.right instanceof RelNode) {
          continue;
        }
        if (j++ == 0) {
          s.append("(");
        } else {
          s.append(", ");
        }
        s.append(value.left).append("=[").append(value.right).append("]");
      }
      if (j > 0) {
        s.append(")");
      }
    }
    // ===== Added this code to print hints ========
    if (withHints && rel instanceof Hintable hintable) {
      var j = 0;
      for (RelHint hint : hintable.getHints()) {
        if (j++ == 0) {
          s.append(" hints[");
        } else {
          s.append(", ");
        }
        s.append(hint.hintName);
        if (!hint.listOptions.isEmpty()) {
          s.append(" options:").append(hint.listOptions);
        }
      }
      if (j > 0) {
        s.append("]");
      }
    }
    // ===== end of added code =====

    switch (detailLevel) {
      case ALL_ATTRIBUTES:
        s.append(": rowcount = ")
            .append(mq.getRowCount(rel))
            .append(", cumulative cost = ")
            .append(mq.getCumulativeCost(rel));
    }
    pw.println(s);
    spacer.add(2);
    explainInputs(inputs);
    spacer.subtract(2);
  }

  /**
   * Copied verbatim from {@link RelWriterImpl}
   *
   * @param inputs
   */
  private void explainInputs(List<RelNode> inputs) {
    for (RelNode input : inputs) {
      input.explain(this);
    }
  }
}
