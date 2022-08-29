# JSON Output

This document explains the JSON structures produced by the SQRL error handler.

## Diagnostics

Diagnostics have the following format:

```javascript
{
/* The primary label of the error */
"label": "",
/* A unique identifier for the error */
"code": "",
/* The severity of the error.
 Values may take on of:
 - "error": A fatal error that prevents compilation
 - "warning": A non-fatal concern
 - "help": A suggestion for improvement
*/
"level": "",
/* The location of the file which caused this diagnostic */
"location": {
    "file_name": "",
    "line_start": "",
    "line_end": "",
    "column_start": "",
    "column_end": "",
    /* The specific text of the file where the diagnostic originated */
    "text": {
        "text": "",
        "highlight_start": "",
        "highlight_end": ""
    }
},
/* A help object containing a short message and a link to docs.
 Linked to a dignostic by the error code. */
"help": {
    /* Pointer to a plaintext file containing 
    an explanation of the error and a few short examples */
    "file_name": "",
    /* Link to online docs with a more thorough explanation of the error */
    "URL":  ""
    }
}
```