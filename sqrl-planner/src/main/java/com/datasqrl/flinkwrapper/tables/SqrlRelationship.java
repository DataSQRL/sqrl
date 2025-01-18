package com.datasqrl.flinkwrapper.tables;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.flinkwrapper.analyzer.RelNodeAnalysis;
import com.datasqrl.flinkwrapper.analyzer.TableAnalysis;
import com.datasqrl.schema.Multiplicity;
import com.datasqrl.schema.Relationship.JoinType;
import java.util.List;
import java.util.function.Supplier;
import lombok.Getter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.FunctionParameter;

@Getter
public class SqrlRelationship extends AnnotatedSqrlTableFunction {

  private final NamePath absolutePath;
  private final JoinType joinType;
  private final Multiplicity multiplicity;

  public SqrlRelationship(Name name,
      List<FunctionParameter> parameters,
      Supplier<RelNode> viewTransform, NamePath fullPath,
      boolean isTest, TableAnalysis tableAnalysis,
      NamePath absolutePath, JoinType joinType, Multiplicity multiplicity) {
    super(parameters, viewTransform, name, fullPath, isTest, tableAnalysis);
    this.absolutePath = absolutePath;
    this.joinType = joinType;
    this.multiplicity = multiplicity;
  }
}
