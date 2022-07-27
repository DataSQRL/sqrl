package org.apache.calcite.sql;

import lombok.Value;
import org.apache.calcite.sql.validate.SqlQualified;

@Value
 public class InlineAggExtractResult {
    SqlIdentifier newIdentifier;
    SqlNode query;
    SqlNode condition;
    SqlNode selectListOrigin;
    SqlQualified qualifiedOrigin;
  }