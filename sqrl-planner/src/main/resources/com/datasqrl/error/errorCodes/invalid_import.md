Invalid IMPORT statement.

To import all tables from another script inline and reference it directly, use:
```
IMPORT ecommerce-data.*;
```
To import all tables from another script in their own namespace, use:
```
IMPORT ecommerce-data;
```
To import all tables from another script with a custom namespace, use:
```
IMPORT ecommerce-data AS catalog;
```

To import from a shared script into your main script, simply use its defined name:
```
IMPORT shared-catalog;
```
To import from a shared script into a submodule script, use the `root` prefix:
```
IMPORT root.shared-catalog;
```
