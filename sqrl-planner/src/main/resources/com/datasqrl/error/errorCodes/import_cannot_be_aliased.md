IMPORT cannot be aliased for star (*) imports.

Erroneous code example:
```
IMPORT ecommerce-data.* AS data;
```

This may be a mistake, perhaps you wanted to alias a specific import. Example:
```
IMPORT ecommerce-data.Orders AS data;
```
