# Text Functions

| Function | Description | Example |
|----------|-------------|---------|
| `format(String, String...)` | A function that formats text based on a format string and variable number of arguments. It uses Java's `String.format` method internally. If the input text is `null`, it returns `null`. | `format("Hello %s!", "World") returns "Hello World!"` |
| `split(String, String)` | Splits the text around matches of the delimiter and returns an array of strings. Replaces Flink's built-in `SPLIT`. The delimiter is a Java regular expression, trailing empty strings are kept, and an empty delimiter splits the text into single characters. Returns `null` if either argument is `null`. See the notes below for escaping and PostgreSQL behavior. | `split('a,b,,c', ',') returns ['a', 'b', '', 'c']` |
| `text_search(String, String...)` | Evaluates a query against multiple text fields and returns a score based on the frequency of query words in the texts. It tokenizes both the query and the texts, and scores based on the proportion of query words found in the text. | `text_search("hello", "hello world") returns 1.0` |

## `split` notes

* Flink SQL string literals do not process backslash escapes, so escape a special character in the delimiter with a single backslash: `'\|'` (or use a character class, `'[|]'`). `'\\|'` splits the same way as `'|'`.
* In a query executed by PostgreSQL (e.g. pinned with `/*+engine(postgres)*/`), `split` maps to `STRING_TO_ARRAY` and the delimiter is literal: `split('a|b|c', '|')` returns `["a","b","c"]` there, but `["a","|","b","|","c",""]` in Flink.
