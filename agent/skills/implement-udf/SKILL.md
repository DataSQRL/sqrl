---
name: implement-udf
description: Use when SQRL/SQL alone cannot implement required logic. Use to create custom scalar, aggregate, or table functions in Java.
---

# Implement User Defined Function

## Implement New UDF 

Only implement a UDF if the functionality cannot be implemented with existing Flink SQL or DataSQRL functions. Look up existing functions first.

Follow these steps:
- From the project root directory, initialize function with `/opt/agent/cmd.sh add-func <function-name>` to create a scalar UDF. Append `--aggregate` to create an aggregate function.
- New function implementation is added in the `functions/` folder with the name `<function-name>.java`
- Import into SQRL script with `IMPORT functions.<function-name>` or `IMPORT functions.*` for all functions. `functions/` sits at the project root, so from a script in a **subfolder** use the reserved `root` prefix — `IMPORT root.functions.<function-name>` — since SQRL relative dot-paths cannot climb upward. (Inside an included project, keep the import relative so it travels with the copied files.)

## Edit UDF

Follow these steps:
- Edit the `.java` file in the `functions/` folder
- Try to use standard java libraries
- Use JBANG `//DEPS` to include dependencies
- Implement tests for the function in the `main` method
- Execute the `main` method after each edit to ensure function compiles and works correctly