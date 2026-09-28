# OpenAI Functions

Import synchronous functions with `IMPORT stdlib.openai.*;` or their asynchronous counterparts with `IMPORT stdlib.openai_async.*;`. Both libraries expose the same function names and require `OPENAI_API_KEY`; the asynchronous library is appropriate for external calls in streaming pipelines.

| Function | Description | Requirement |
|----------|-------------|-------------|
| `completions(String prompt, String model_name)` | Generates a completion for the given prompt using the specified OpenAI model. For example, `completions('What is AI?', 'gpt-4o')` returns a possible response to the prompt. | Set OPENAI_API_KEY environment variable |
| `completions(String prompt, String model_name, Integer maxOutputTokens)` | Generates a completion for the given prompt using the specified OpenAI model, with an upper limit on output tokens. | Set OPENAI_API_KEY environment variable |
| `completions(String prompt, String model_name, Integer maxOutputTokens, Double temperature)` | Generates a completion with an output-token limit and temperature. | Set OPENAI_API_KEY environment variable |
| `completions(String prompt, String model_name, Integer maxOutputTokens, Double temperature, Double topP)` | Generates a completion with an output-token limit, temperature, and top-p value. | Set OPENAI_API_KEY environment variable |
| `extract_json(String prompt, String model_name)` | Extracts JSON data from the given prompt using the specified OpenAI model. For example, `extract_json('What is AI?', 'gpt-4o')` returns any relevant JSON data for the prompt. | Set OPENAI_API_KEY environment variable |
| `extract_json(String prompt, String model_name, Double temperature)` | Extracts JSON data from the given prompt using the specified OpenAI model and a specified temperature. For example, `extract_json('What is AI?', 'gpt-4o', 0.5)` returns any relevant JSON data for the prompt, weighted by a temperature of 0.5. | Set OPENAI_API_KEY environment variable |
| `extract_json(String prompt, String model_name, Double temperature, Double topP)` | Extracts JSON data from the given prompt using the specified OpenAI model, with a specified temperature and top-p value. For example, `extract_json('What is AI?', 'gpt-4o', 0.5, 0.9)` returns any relevant JSON data for the prompt, weighted by a temperature of 0.5 and with a top-p value of 0.9. | Set OPENAI_API_KEY environment variable |
| `extract_json(String prompt, String model_name, Double temperature, Double topP, String jsonSchema)` | Extracts JSON constrained by the supplied JSON Schema. | Set OPENAI_API_KEY environment variable |
| `vector_embed(String text, String model_name)` | Embeds the given text into a vector using the specified OpenAI model. For example, `vector_embed('What is AI?', 'text-embedding-ada-002')` returns a vector representation of the text. | Set OPENAI_API_KEY environment variable |
