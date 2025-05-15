/**
 * Produces a DAG (directed acyclic graph) of all the tables and functions
 * defined in a SQRL project with the {@link com.datasqrl.planner.dag.DAGBuilder} to produce
 * a {@link com.datasqrl.planner.dag.PipelineDAG}.
 * The DAG is optimized and assembled in the {@link com.datasqrl.planner.dag.DAGPlanner}.
 */
package com.datasqrl.planner.dag;