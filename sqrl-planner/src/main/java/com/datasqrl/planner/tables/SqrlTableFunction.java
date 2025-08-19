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
package com.datasqrl.planner.tables;

import com.datasqrl.calcite.SqrlRexUtil;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.engine.ExecutableQuery;
import com.datasqrl.io.tables.TableType;
import com.datasqrl.plan.table.Multiplicity;
import com.datasqrl.planner.analyzer.TableAnalysis;
import com.datasqrl.planner.analyzer.TableOrFunctionAnalysis;
import com.datasqrl.planner.util.Documented;
import com.google.common.base.Preconditions;
import java.lang.reflect.Type;
import java.time.Duration;
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
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.TableFunction;

/**
 * Represents a function in DataSQRL. A function is either defined by the user and generated as a
 * table access function for a defined table.
 */
@AllArgsConstructor
@Getter
@Builder
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
@ToString(onlyExplicitlyIncluded = true)
public class SqrlTableFunction implements TableFunction, TableOrFunctionAnalysis, Documented {

  /**
   * The full path for this function. If it is a root-level function, the path has size 1. If it is
   * a relationship, the path has size 2.
   */
  @Include @ToString.Include @NonNull private final NamePath fullPath;

  /**
   * The (ordered) list of {@link SqrlFunctionParameter} parameters for this function (empty if no
   * parameters)
   */
  @ToString.Include @Default private final List<FunctionParameter> parameters = List.of();

  /**
   * The analysis of the function logic, including rowtype (i.e. the result type of this function),
   * source tables, etc
   */
  @NonNull private final TableAnalysis functionAnalysis;

  /** The multiplicity of the function result set */
  @Default private final Multiplicity multiplicity = Multiplicity.MANY;

  /** The visibility of this function */
  private final AccessVisibility visibility;

  /** After planning, this represents the executable query for this function */
  @Nullable @Default @Setter private ExecutableQuery executableQuery = null;

  /** A documentation string that describes the function */
  @Default private Optional<String> documentation = Optional.empty();

  @Default private Duration cacheDuration = Duration.ZERO;

  @Override
  public RelDataType getRowType(
      RelDataTypeFactory relDataTypeFactory, List<? extends Object> list) {
    return functionAnalysis.getRowType();
  }

  public boolean hasParameters() {
    return !parameters.isEmpty();
  }

  /**
   * Used to register function in catalog. Requires that it's simple
   *
   * @return
   */
  public String getFunctionCatalogName() {
    Preconditions.checkArgument(fullPath.size() == 1);
    return getSimpleName();
  }

  public String getSimpleName() {
    return fullPath.getLast().getDisplay();
  }

  @Override
  public Type getElementType(List<? extends Object> list) {
    return Object[].class;
  }

  public boolean isRelationship() {
    return fullPath.size() > 1;
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
    return limit.filter(i -> i <= 1).map(i -> Multiplicity.ZERO_ONE).orElse(Multiplicity.MANY);
  }

  @Override
  public UniqueIdentifier getIdentifier() {
    return functionAnalysis.getIdentifier();
  }

  @Override
  public TableType getType() {
    return functionAnalysis.getType();
  }

  @Override
  public boolean isSourceOrSink() {
    return false;
  }
}
