package com.datasqrl.flinkwrapper.tables;

import com.datasqrl.calcite.function.SqrlTableMacro;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;

import com.datasqrl.flinkwrapper.analyzer.RelNodeAnalysis;
import com.datasqrl.flinkwrapper.analyzer.TableAnalysis;
import com.datasqrl.schema.Multiplicity;
import com.datasqrl.schema.Relationship.JoinType;
import java.util.List;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;

@Getter
public class AnnotatedSqrlTableFunction extends SqrlTableFunction implements SqrlTableMacro {

  private final Name name;
  private final NamePath fullPath;
  private final boolean isTest;

  public AnnotatedSqrlTableFunction(List<FunctionParameter> parameters,
      Supplier<RelNode> viewTransform, Name name, NamePath fullPath, boolean isTest,
      TableAnalysis tableAnalysis) {
    super(parameters, viewTransform, tableAnalysis);
    this.name = name;
    this.fullPath = fullPath;
    this.isTest = isTest;
  }

  @Override
  public NamePath getAbsolutePath() {
    return fullPath;
  }

  @Override
  public String getDisplayName() {
    return getName().getDisplay();
  }

  @Override
  public Multiplicity getMultiplicity() {
    return Multiplicity.MANY;
  }

  @Override
  public JoinType getJoinType() {
    return JoinType.NONE;
  }

}
