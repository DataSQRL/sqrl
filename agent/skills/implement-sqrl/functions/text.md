# Text Functions

| Function | Description | Example |
|----------|-------------|---------|
| `format(String, String...)` | A function that formats text based on a format string and variable number of arguments. It uses Java's `String.format` method internally. If the input text is `null`, it returns `null`. | `format('Hello %s!', 'World') returns 'Hello World!'` |
| `split(String, String)` | Splits the text around matches of the delimiter and returns an array of strings. Replaces Flink's built-in `SPLIT`. The delimiter is a Java regular expression, trailing empty strings are kept, and an empty delimiter splits the text into single characters. Returns `null` if either argument is `null`. See the notes below for escaping and PostgreSQL behavior. | `split('a,b,,c', ',') returns ['a', 'b', '', 'c']` |
| `text_search(String, String...)` | Evaluates a query against multiple text fields and returns a score based on the frequency of query words in the texts. It tokenizes both the query and the texts, and scores based on the proportion of query words found in the text. See the notes below for PostgreSQL behavior. | `text_search('hello', 'hello world') returns 1.0` |

## `split` notes

* Flink SQL string literals do not process backslash escapes, so escape a special character in the delimiter with a single backslash: `'\|'` (or use a character class, `'[|]'`). `'\\|'` splits the same way as `'|'`.
* In a query executed by PostgreSQL (e.g. pinned with `/*+engine(postgres)*/`), `split` maps to `STRING_TO_ARRAY` and the delimiter is literal: `split('a|b|c', '|')` returns `["a","b","c"]` there, but `["a","|","b","|","c",""]` in Flink.

## `text_search` notes

* Flink and PostgreSQL evaluate `text_search` differently, so the same query can return different rows and scores depending on the engine that executes it:
  * Flink splits the query and the texts on whitespace, lowercases the words, and returns the fraction of query words found: `text_search('garden hat', 'garden gnome')` returns `0.5`.
  * In a query executed by PostgreSQL, `text_search` maps to PostgreSQL full-text search with the `english` configuration: `to_tsvector` over the texts, matched against `websearch_to_tsquery` of the query, and scored with `ts_rank_cd`. Words are stemmed and stop words are ignored, the query accepts web search syntax (`"exact phrase"`, `or`, `-word`), and a row matches only if it contains every query word: `text_search('garden hat', 'garden gnome') > 0` is false there. Scores are not normalized: `text_search('hello', 'hello world')` returns `0.1`.
* In PostgreSQL, filter with `text_search(...) > 0` and order by the score to rank the matches. Any other comparison fails compilation with `Expected comparison with 0 for TextSearch`.
* The compiler creates a GIN index for the columns of a `text_search(...) > 0` filter, so the search does not scan the whole table. A query that only computes or orders by the score, without that filter, scans every row. Pass the same columns in the same order to every `text_search` on a table: an index only serves searches over exactly its column list.
