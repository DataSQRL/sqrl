Nested DISTINCT ON statements not supported.

Erroneous code example:
```
IMPORT ecommerce-data.Product;
Product.nested := DISTINCT @ ON @.productid;
```