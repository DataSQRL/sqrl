package com.datasqrl.v2.tables;

import com.datasqrl.calcite.SqrlRexUtil;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.engine.ExecutableQuery;
import com.datasqrl.v2.analyzer.TableAnalysis;
import com.datasqrl.v2.analyzer.TableOrFunctionAnalysis;
import com.datasqrl.schema.Multiplicity;
import com.google.common.base.Preconditions;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.EqualsAndHashCode;
import lombok.EqualsAndHashCode.Include;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
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
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
@ToString(onlyExplicitlyIncluded = true)
public class SqrlTableFunction implements TableFunction, TableOrFunctionAnalysis {

  /**
   * The full path for this function. If it is a root-level
   * function, the path has size 1. If it is a relationship, the path has size 2.
   */
  @Include @ToString.Include
  private final NamePath fullPath;
  /**
   * The (ordered) list of {@link SqrlFunctionParameter} parameters for this function (empty if no parameters)
   */
  @Include @ToString.Include @Default
  private final List<FunctionParameter> parameters = List.of();
  /**
   * The analysis of the function logic, including rowtype (i.e. the result type of this function), source tables, etc
   */
  private final TableAnalysis functionAnalysis;
  /**
   * The base table on which this function is defined.
   * This means, that this function returns the same type as the base table.
   */
  @Default
  private final Optional<TableAnalysis> baseTable = Optional.empty();
  /**
   * The multiplicity of the function result set
   */
  @Default
  private final Multiplicity multiplicity = Multiplicity.MANY;


  /**
   * The visibility of this function
   */
  private final AccessVisibility visibility;

  /**
   * After planning, this represents the executable query for this function
   */
  @Nullable @Default @Setter
  private ExecutableQuery executableQuery = null;

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory, List<? extends Object> list) {
    return functionAnalysis.getRowType();
  }

  public boolean hasParameters() {
    return !parameters.isEmpty();
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
