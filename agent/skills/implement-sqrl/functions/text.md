# Text Functions

| Function | Description | Example |
|----------|-------------|---------|
| `format(String, String...)` | A function that formats text based on a format string and variable number of arguments. It uses Java's `String.format` method internally. If the input text is `null`, it returns `null`. | `format("Hello %s!", "World") returns "Hello World!"` |
| `split(String, String)` | Splits the text around matches of the delimiter and returns an array of strings. The delimiter is a Java regular expression (escape special characters such as `.` or the pipe character), trailing empty strings are kept, and an empty delimiter splits the text into single characters. Returns `null` if either argument is `null`. | `split('a,b,,c', ',') returns ['a', 'b', '', 'c']` |
| `text_search(String, String...)` | Evaluates a query against multiple text fields and returns a score based on the frequency of query words in the texts. It tokenizes both the query and the texts, and scores based on the proportion of query words found in the text. | `text_search("hello", "hello world") returns 1.0` |
