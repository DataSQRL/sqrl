# Text Functions

| Function | Description | Example |
|----------|-------------|---------|
| `format(String, String...)` | A function that formats text based on a format string and variable number of arguments. It uses Java's `String.format` method internally. If the input text is `null`, it returns `null`. | `format("Hello %s!", "World") returns "Hello World!"` |
| `text_search(String, String...)` | Evaluates a query against multiple text fields and returns a score based on the frequency of query words in the texts. It tokenizes both the query and the texts, and scores based on the proportion of query words found in the text. | `text_search("hello", "hello world") returns 1.0` |
