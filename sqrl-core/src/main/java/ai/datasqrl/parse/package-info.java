/**
 * A parser is a tool that converts text into an abstract syntax tree. This parser uses ANTLR
 * grammar to produce an Abstract Syntax Tree (AST) that represents the syntactic structure of the
 * text.
 * <p>
 * This parser is heavily derived from Presto, all classes in the parser package are explicitly
 * licensed Apache 2.
 * <p>
 * SQRL specific statements include expression assignments, join declarations, query assignments,
 * subscriptions, import statements, and export statements.
 * <p>
 * Expression assignments are shorthand to add a column to a table using only an expression.
 * <p>
 * Join declarations define a path through the schema. Join declarations will point to the most
 * recent version of that table, with any fields added after the declaration being visible.
 * <p>
 * Query assignments are used to create tables.
 * <p>
 * Subscriptions are used to listen to changes in data.
 * <p>
 * Import statements are used to import datasets from the dataset registry or import functions.
 * <p>
 * Export statements are used to export tables to the dataset registry.
 */
package ai.datasqrl.parse;