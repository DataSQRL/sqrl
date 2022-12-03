/**
 * Welcome to DataSQRL!
 * <p>
 * You'll be able to learn more about the project in the package-info.java files in each package.
 * <p>
 * The components are:
 * <p>
 * <p>- The Parser is responsible for converting SQRL source code into an abstract syntax tree
 * (AST).
 * <p>- The Transpiler is responsible for converting an AST into a normalized intermediate form and
 * then into standard SQL.
 * <p>- The Planner is responsible for converting standard SQL into a logical plan.
 * <p>- The Optimizer is responsible for optimizing the logical plan for performance.
 * <p>- The streaming engine is responsible for executing optimized SQRL plans.
 * <p>- The API is responsible for giving the user queryable access to their data.
 * <p>- The Server is responsible for managing SQRL scripts and providing an interface for
 * developers to interact with SQRL.
 * <p>
 * <p>
 * As an analogy: The SQRL Cake Maker is a tool that enables users to create custom cake recipes. It
 * consists of a parser that converts a recipe into a list of ingredients, a transpiler that cuts
 * and prepares a set of raw ingredients for the needs of the recipe, a planner that converts the
 * ingredients into a cake, and an optimizer that optimizes the cake for taste. The SQRL Cake Maker
 * also includes a streaming engine that enables users to bake the cake, and an API that allows
 * users to serve pieces of the cake. Finally, the Cake Maker includes a server that manages cake
 * recipes and enables users to share their recipes with others.
 */
package com.datasqrl;