/**
 * Produces a DAG (directed acyclic graph) of all the tables and functions
 * defined in a SQRL project with the {@link com.datasqrl.v2.dag.DAGBuilder} to produce
 * a {@link com.datasqrl.v2.dag.PipelineDAG}.
 * The DAG is optimized and assembled in the {@link com.datasqrl.v2.dag.DAGPlanner}.
 */
package com.datasqrl.v2.dag;