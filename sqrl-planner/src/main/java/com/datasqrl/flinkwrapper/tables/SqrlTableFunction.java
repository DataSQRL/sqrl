package com.datasqrl.flinkwrapper.tables;

import com.datasqrl.calcite.SqrlRexUtil;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.flinkwrapper.analyzer.TableAnalysis;
import com.datasqrl.flinkwrapper.analyzer.TableOrFunctionAnalysis;
import com.datasqrl.schema.Multiplicity;
import com.google.common.base.Preconditions;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.TableFunction;
import org.apache.flink.table.catalog.ObjectIdentifier;

@AllArgsConstructor
@Getter
@Builder
public class SqrlTableFunction implements TableFunction, TableOrFunctionAnalysis {

  /**
   * The analysis of the function logic, including rowtype (i.e. the result type of this function), source tables, etc
   */
  private final TableAnalysis functionAnalysis;
  /**
   * The (ordered) list of {@link SqrlFunctionParameter} parameters for this function (empty if no parameters)
   */
  private final List<FunctionParameter> parameters;
  /**
   * The base table on which this function is defined.
   * This means, that this function returns the same type as the base table.
   */
  private final Optional<TableAnalysis> baseTable;
  /**
   * The multiplicity of the function result set
   */
  @Default
  private final Multiplicity multiplicity = Multiplicity.MANY;

  /**
   * The full path for this function. If it is a root-level
   * function, the path has size 1. If it is a relationship, the path has size 2.
   */
  private final NamePath fullPath;
  /**
   * The visibility of this function
   */
  private final AccessVisibility visibility;

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory, List<? extends Object> list) {
    return functionAnalysis.getRowType();
  }

  public String getFunctionCatalogName() {
    Preconditions.checkArgument(fullPath.size()==1);
    return fullPath.getLast().getDisplay();
  }

  @Override
  public Type getElementType(List<? extends Object> list) {
    return Object[].class;
  }

  public boolean isRelationship() {
    return fullPath.size()>1;
  }

  @Override
  public RelNode getRelNode() {
    return functionAnalysis.getRelNode();
  }

  public RelDataType getRowType() {
    return getRowType(null, null);
  }

  public Supplier<RelNode> getViewTransform() {
    return functionAnalysis::getCollapsedRelnode;
  }

  public static Multiplicity getMultiplicity(RelNode relNode) {
    Optional<Integer> limit = Optional.empty();
    if (relNode instanceof Sort) {
      limit = SqrlRexUtil.getLimit(((Sort)relNode).fetch);
    }
    return limit.filter(i -> i<=1).map(i -> Multiplicity.ZERO_ONE).orElse(Multiplicity.MANY);
  }

  @Override
  public ObjectIdentifier getIdentifier() {
    return functionAnalysis.getIdentifier();
  }
}
