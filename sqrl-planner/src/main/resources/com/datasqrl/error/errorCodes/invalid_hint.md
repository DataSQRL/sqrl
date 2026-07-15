Invalid SQL hint provided.

A SQL hint comment block has the following structure:
```
/*+ hint_without_arg, hint_with_args(arg1, arg2) */
```
Make sure a hint comment block contains ONLY hints, separated by commas and no other comments.
Move any additional comments into a separate comment block.
