# JSON Output

This document explains the JSON structures produced by the SQRL error handler.

## Diagnostics

Diagnostics have the following format:

```javascript
{
/* The primary label of the error */
"label": "Column `id` ambiguous",
/* A unique identifier for the error */
"code": "0001",
/* The severity of the error.
 Values may take one of:
 - "fatal": A fatal error that prevents compilation
 - "warning": A non-fatal concern
 - "information": A suggestion for improvement
*/
"level": "fatal",
/* The location of the file which caused this diagnostic */
"location": {
    "file_name": "examples/testScript.sqrl",
    "line_start": "12",
    "line_end": "12",
    "column_start": "7",
    "column_end": "9",
    /* The specific text of the file where the diagnostic originated */
    "text": {
        "text": "SELECT id, lastName, price, quantity, sku FROM customers c JOIN orders o ON c.orderNo = o.orderNo",
        "highlight_start": "7",
        "highlight_end": "9"
    }
},
/* A help object containing a short message and a link to docs.
 Linked to a dignostic by the error code. */
"help": {
    /* Pointer to a plaintext file containing 
    an explanation of the error and a few short examples */
    "file_name": "sqrlDocs/help/0001.txt",
    /* Link to online docs with a more thorough explanation of the error */
    "URL":  "https://datasqrl.github.io/docs/errorCodes#0001"
    }
}
```

# Map *from* OpenAPI

Corresponds with the existing ErrorMessage object in the file: datasqrl-openai.yml.

```javascript
{
"code":
"message":
"severity":
"location": 
    {
    "prefix":
    "path":
    "file":
        {
        "line":
        "offset":
        }
    }
}
```

# Map *to* OpenAPI

Maps the first JSON structure (under 'diagnostics') to yaml format, following datasqrl-openapi.yml.

```yaml
ErrorPayload:
  type: object
  required:
    - message
    - severity
  properties:
    message:
      $ref: 'ErrorMessage'
    severity:
      $ref: 'ErrorSeverity'
    location:
      $ref: 'ErrorLocation'
    help:
      $ref: 'ErrorHelp'
      
ErrorMessage:
  type: object
  required:
    - code
  properties:
    code:
      type: integer
      format: int32
      nullable: false
    message:
      type: string
      
ErrorSeverity:
  type: string
  enum:
    - fatal
    - warning
    - information
    
ErrorLocation:
  type: object
  properties:
    prefix:
      type: string
    path:
        type: string
    file:
      type: object
      properties:
        text:
          type: string
        line_start:
          type: integer
        line_end:
          type: integer
        offset_start:
          type: integer
        offset_end:
          type: integer
        highlight_start:
          type: integer
        highlight_end:
          type: integer
          
ErrorHelp:
  type: object
  properties:
    path:
      type: string
    text:
      type: string
    docsUrl: 
      type:
        string
```