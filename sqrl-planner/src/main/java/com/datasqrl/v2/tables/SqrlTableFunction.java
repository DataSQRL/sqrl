package com.datasqrl.v2.tables;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.TableFunction;
import org.apache.flink.table.catalog.ObjectIdentifier;

import com.datasqrl.calcite.SqrlRexUtil;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.engine.ExecutableQuery;
import com.datasqrl.io.tables.TableType;
import com.datasqrl.schema.Multiplicity;
import com.datasqrl.v2.analyzer.TableAnalysis;
import com.datasqrl.v2.analyzer.TableOrFunctionAnalysis;
import com.datasqrl.v2.util.Documented;
import com.google.common.base.Preconditions;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.EqualsAndHashCode;
import lombok.EqualsAndHashCode.Include;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;

/**
 * Represents a function in DataSQRL.
 * A function is either defined by the user and generated as a table access function for a
 * defined table.
 */
@AllArgsConstructor
@Getter
@Builder
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
@ToString(onlyExplicitlyIncluded = true)
public class SqrlTableFunction implements TableFunction, TableOrFunctionAnalysis, Documented {

  /**
   * The full path for this function. If it is a root-level
   * function, the path has size 1. If it is a relationship, the path has size 2.
   */
  @Include @ToString.Include @NonNull
  private final NamePath fullPath;
  /**
   * The (ordered) list of {@link SqrlFunctionParameter} parameters for this function (empty if no parameters)
   */
  @ToString.Include @Default
  private final List<FunctionParameter> parameters = List.of();
  /**
   * The analysis of the function logic, including rowtype (i.e. the result type of this function), source tables, etc
   */
  @NonNull
  private final TableAnalysis functionAnalysis;
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

  /**
   * A documentation string that describes the function
   */
  @Default
  private Optional<String> documentation = Optional.empty();

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

  @Override
public RelDataType getRowType() {
    return getRowType(null, null);
  }

  @Override
  public TableAnalysis getBaseTable() {
    return functionAnalysis.getBaseTable();
  }

  public Supplier<RelNode> getViewTransform() {
    return functionAnalysis::getCollapsedRelnode;
  }

  public static Multiplicity getMultiplicity(RelNode relNode) {
    Optional<Integer> limit = Optional.empty();
    if (relNode instanceof Sort sort) {
      limit = SqrlRexUtil.getLimit(sort.fetch);
    }
    return limit.filter(i -> i<=1).map(i -> Multiplicity.ZERO_ONE).orElse(Multiplicity.MANY);
  }

  @Override
  public ObjectIdentifier getIdentifier() {
    return functionAnalysis.getIdentifier();
  }

  @Override
  public List<String> getParameterNames() {
    return parameters.stream().map(FunctionParameter::getName).collect(Collectors.toList());
  }

  @Override
  public TableType getType() {
    return functionAnalysis.getType();
  }
}
