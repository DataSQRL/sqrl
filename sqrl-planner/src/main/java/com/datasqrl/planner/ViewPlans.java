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
package com.datasqrl.planner;

import java.util.LinkedHashSet;
import java.util.Set;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.QueryOperationCatalogView;
import org.apache.flink.table.catalog.ResolvedCatalogView;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;

/**
 * Lets the queries of the scripts reuse the plans of the views that the scripts define.
 *
 * <p>Flink expands a view each time a query uses it: it parses, validates and converts the SQL of
 * the view again, and so that of every view the view uses. Each table of a script is a view of the
 * tables before it, so each query plans its chains of tables again, which makes planning slow for
 * long scripts. While the scripts are planned, each view is therefore shadowed by a temporary view
 * with the same name, schema and SQL that holds the plan of the view, which Flink uses as it is.
 *
 * <p>Flink compiles the pipeline with the views themselves, see {@link #restoreViews()}: its
 * optimizer expects each use of a view to be a tree of its own, e.g. with its own correlation
 * variables and time attributes.
 */
final class ViewPlans {

  private final CatalogManager catalogManager;

  /** The views that are shadowed by their plans */
  private final Set<ObjectIdentifier> shadowed = new LinkedHashSet<>();

  private boolean restored = false;

  ViewPlans(CatalogManager catalogManager) {
    this.catalogManager = catalogManager;
  }

  /**
   * Lets queries use the plan of a view that was created or altered. A temporary view is not
   * shadowed, since there can only be one temporary object of a name.
   *
   * @param view the view
   * @param plan the plan that Flink expands the view to: its converted query, to which Flink adds
   *     the cast to the schema of the view either way
   */
  void reuse(ObjectIdentifier view, RelNode plan) {
    if (restored) {
      return;
    }
    forget(view);
    var table = catalogManager.getTableOrError(view);
    if (table.isTemporary()
        || !(table.getResolvedTable() instanceof ResolvedCatalogView definition)) {
      return;
    }
    var operation = new PlannerQueryOperation(plan, definition::getExpandedQuery);
    // With the schema of the view, which Flink would otherwise derive from the plan
    var shadow =
        new ResolvedCatalogView(
            new QueryOperationCatalogView(operation, definition.getOrigin()),
            definition.getResolvedSchema());
    catalogManager.createTemporaryTable(shadow, view, false);
    shadowed.add(view);
  }

  /**
   * Makes queries expand a view again, e.g. before it is altered, which Flink does not allow while
   * a temporary view shadows it.
   */
  void forget(ObjectIdentifier view) {
    if (shadowed.remove(view)) {
      catalogManager.dropTemporaryView(view, false);
    }
  }

  /** Makes queries expand all views again, for good, before Flink compiles the pipeline. */
  void restoreViews() {
    Set.copyOf(shadowed).forEach(this::forget);
    restored = true;
  }
}
