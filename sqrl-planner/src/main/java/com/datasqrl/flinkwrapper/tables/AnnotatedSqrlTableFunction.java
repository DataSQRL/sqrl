package com.datasqrl.flinkwrapper.tables;

import com.datasqrl.calcite.function.SqrlTableMacro;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;

import com.datasqrl.flinkwrapper.analyzer.RelNodeAnalysis;
import com.datasqrl.flinkwrapper.analyzer.TableAnalysis;
import com.datasqrl.flinkwrapper.planner.AccessVisibility;
import com.datasqrl.schema.Multiplicity;
import com.datasqrl.schema.Relationship.JoinType;
import java.util.List;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;

@Getter
public class AnnotatedSqrlTableFunction extends SqrlTableFunction implements SqrlTableMacro {

  private final NamePath fullPath;
  private final AccessVisibility visibility;

  @Setter
  private Supplier<RelNode> viewTransform = null;


  public AnnotatedSqrlTableFunction(SqrlTableFunction function,
      NamePath fullPath, AccessVisibility visibility) {
    super(function.getParameters(), function.getRowType(), function.getTableAnalysis());
    this.fullPath = fullPath;
    this.visibility = visibility;
  }

  public Name getName() {
    return fullPath.getLast();
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

  @Override
  public boolean isTest() {
    return visibility.isTest();
  }

}
