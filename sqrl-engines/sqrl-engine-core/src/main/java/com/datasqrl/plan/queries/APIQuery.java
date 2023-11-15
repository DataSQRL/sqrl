/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.queries;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.function.SqrlFunctionParameter;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.EqualsAndHashCode.Include;
import lombok.ToString;
import lombok.Value;
import org.apache.calcite.rel.RelNode;

@Value
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
@ToString
public class APIQuery implements IdentifiedQuery {

  @Include
  private final String nameId;
  private final RelNode relNode;
  private final List<SqrlFunctionParameter> parameterList;
  private final NamePath namePath;
  private final boolean isPermutation;

}
