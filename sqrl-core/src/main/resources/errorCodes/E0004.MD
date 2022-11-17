IMPORT statements must be in header

Erroneous code example:
```
IMPORT ecommerce-data.Orders;
Orders.entries.discount := discount ? 0.0;

IMPORT ecommerce-data.Product;
```

Rearrange the imports so they are in the header. Example:
```
IMPORT ecommerce-data.Orders;
IMPORT ecommerce-data.Product;

Orders.entries.discount := discount ? 0.0;
```
