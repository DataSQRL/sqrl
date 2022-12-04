/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
/**
 * This module is responsible for planning AST node to logical plan Dags.
 * <p>
 * Taking a Node from the parser, the Node is converted to an intermediate representation that uses
 * Norm Nodes. The intermediate representation is converted to Calcite's SqlNode. Apache Calcite is
 * used to optimize the SqlNode into a logical plan, and optionally converted to an in-memory
 * executable plan. The logical plans for each statement are then assembled into a dag.
 */
package com.datasqrl.plan;