DataSQRL pulls up any sort order for table definitions to execute them at query time
for performance reasons.
This requires that all sort column are in the SELECT clause. You can hide these columns
with `_` or use table functions.