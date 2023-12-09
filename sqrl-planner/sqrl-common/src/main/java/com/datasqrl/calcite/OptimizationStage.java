/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.calcite;

import com.datasqrl.plan.SQRLPrograms;
import lombok.Value;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * An {@link OptimizationStage}
 */
@Value
public class OptimizationStage {

  private static final List<OptimizationStage> ALL_STAGES = new ArrayList<>();

  private final int index;
  private final String name;
  private final Program program;
  private final Optional<RelTrait> trait;

  public OptimizationStage(String name, Program program, Optional<RelTrait> trait) {
    this.name = name;
    this.program = program;
    this.trait = trait;
    synchronized (ALL_STAGES) {
      this.index = ALL_STAGES.size();
      ALL_STAGES.add(index, this);
    }
  }

  public static List<Program> getAllPrograms() {
    return ALL_STAGES.stream().map(OptimizationStage::getProgram).collect(Collectors.toList());
  }

  public static final OptimizationStage VOLCANO = new OptimizationStage("Volcano",
      SQRLPrograms.ENUMERABLE_VOLCANO, Optional.of(EnumerableConvention.INSTANCE)
  );

  //Enumerable
  public static final OptimizationStage CALCITE_ENGINE = new OptimizationStage("standardEnumerable",
      Programs.sequence(
          Programs.ofRules(Programs.RULE_SET), Programs.CALC_PROGRAM,
          Programs.ofRules(EnumerableRules.ENUMERABLE_RULES)),
            Optional.of(EnumerableConvention.INSTANCE));


}
