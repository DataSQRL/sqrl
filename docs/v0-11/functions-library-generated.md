# Library Functions

DataSQRL provides extended library functions that can be imported into your SQRL scripts. These functions offer specialized capabilities for mathematical operations, data processing, AI integration, and external system connectivity.

Library functions must be imported into the SQRL script via one of the following `IMPORT` statements:

```sql
IMPORT stdlib.library-name.*; --imports all functions in the library
IMPORT stdlib.library-name.function-name; --imports a single function by name
IMPORT stdlib.library-name.function-name AS myName; --imports a single function under a given name
```
## Math Functions

| Function | Description | Import     | Requirements |
|----------|-------------|------------|--------------|
| `cbrt(double)` | Calculates the cube root of a number. For example, `cbrt(27.0)` returns `3.0`, which is the cube root of 27.0. | `IMPORT math.cbrt;` |  |
| `copy_sign(double, double)` | Returns the first argument with the sign of the second argument. For example, `copy_sign(2.0, -3.0)` returns `-2.0`. | `IMPORT math.copy_sign;` |  |
| `expm1(double)` | Calculates e^x - 1 with better precision for small values. For example, `expm1(0.0)` returns `0.0`, as `e^0 - 1 = 0`. | `IMPORT math.expm1;` |  |
| `hypot(double, double)` | Computes sqrt(x² + y²) without intermediate overflow or underflow. For example, `hypot(3.0, 4.0)` returns `5.0`, which is the hypotenuse of a 3-4-5 triangle. | `IMPORT math.hypot;` |  |
| `log1p(double)` | Computes the natural logarithm of 1 + x (log(1 + x)) accurately for small x. For example, `log1p(0.0)` returns `0.0` as `log(1 + 0) = 0`. | `IMPORT math.log1p;` |  |
| `next_after(double, double)` | Returns the next floating-point number towards the direction of the second argument. For example, `next_after(1.0, 2.0)` returns the next representable number after 1.0. | `IMPORT math.next_after;` |  |
| `scalb(double, bigint)` | Multiplies a floating-point number by 2 raised to the power of an integer. For example, `scalb(1.0, 3)` returns `8.0` as `1.0 * 2^3 = 8.0`. | `IMPORT math.scalb;` |  |
| `ulp(double)` | Returns the size of the unit in the last place (ULP) of the argument. For example, `ulp(1.0)` returns the ULP of 1.0. | `IMPORT math.ulp;` |  |
| `binomial_distribution(bigint, double, bigint)` | Calculates the probability of obtaining a number of successes in a fixed number of trials for a binomial distribution. For example, `binomial_distribution(10, 0.5, 5)` returns the probability of 5 successes out of 10 trials with a 50% success rate. | `IMPORT math.binomial_distribution;` |  |
| `exponential_distribution(double, double)` | Evaluates the probability density or cumulative distribution of an exponential distribution. For example, `exponential_distribution(1.0, 2.0)` returns the exponential distribution's probability for a given rate and time. | `IMPORT math.exponential_distribution;` |  |
| `normal_distribution(double, double, double)` | Evaluates the cumulative distribution function for a normal (Gaussian) distribution. For example, `normal_distribution(0.0, 1.0, 1.0)` returns the probability for a standard normal distribution with mean 0 and standard deviation 1. | `IMPORT math.normal_distribution;` |  |
| `poisson_distribution(double, bigint)` | Evaluates the probability mass function of a Poisson-distributed random variable. For example, `poisson_distribution(1.0, 5)` returns the probability of observing 5 events when the average event rate is 1.0. | `IMPORT math.poisson_distribution;` |  |

## Iceberg Functions

| Function | Description | Import     | Requirements |
|----------|-------------|------------|--------------|
| `read_partition_sizes(String warehouse, String catalogType, String catalogName, String databaseName, String tableName)` | Calculates the total size on disk in bytes for each partition in an Iceberg table. | `IMPORT iceberg.read_partition_sizes;` |  |
| `delete_duplicated_data(String warehouse, String catalogType, String catalogName, String databaseName, String tableName, Long maxTimeBucket, Map[Map[String, String], Integer] partitionSet)` | Deletes duplicated data from an Iceberg table based on partition specifications and time bucket constraints. | `IMPORT iceberg.delete_duplicated_data;` |  |

## Openai Functions

| Function | Description | Import     | Requirements |
|----------|-------------|------------|--------------|
| `completions(String prompt, String model_name)` | Generates a completion for the given prompt using the specified OpenAI model. For example, `completions('What is AI?', 'gpt-6-luna')` returns a possible response to the prompt. | `IMPORT openai.completions;` | Set OPENAI_API_KEY environment variable |
| `completions(String prompt, String model_name, Integer maxOutputTokens)` | Generates a completion for the given prompt using the specified OpenAI model, with an upper limit on the number of output tokens. For example, `completions('What is AI?', 'gpt-6-luna', 100)` returns a possible response to the prompt, limited to 100 characters. | `IMPORT openai.completions;` | Set OPENAI_API_KEY environment variable |
| `completions(String prompt, String model_name, Integer maxOutputTokens, Double temperature)` | Generates a completion for the given prompt using the specified OpenAI model, with an upper limit on the number of output tokens and a specified temperature. With GPT-6 Luna, use the overload that also accepts reasoningEffort and set it to `none`. | `IMPORT openai.completions;` | Set OPENAI_API_KEY environment variable |
| `completions(String prompt, String model_name, Integer maxOutputTokens, Double temperature, Double topP)` | Generates a completion for the given prompt using the specified OpenAI model, with an upper limit on the number of output tokens, a specified temperature, and a specified top-p value. With GPT-6 Luna, use the overload that also accepts reasoningEffort and set it to `none`. | `IMPORT openai.completions;` | Set OPENAI_API_KEY environment variable |
| `completions(String prompt, String model_name, Integer maxOutputTokens, Double temperature, Double topP, String reasoningEffort)` | Generates a completion with a reasoning-effort setting. For GPT-6 Luna custom sampling requires reasoningEffort to be `none`; for example, `completions('What is AI?', 'gpt-6-luna', 100, 0.5, 1.0, 'none')`. | `IMPORT openai.completions;` | Set OPENAI_API_KEY environment variable |
| `extract_json(String prompt, String model_name)` | Extracts JSON data from the given prompt using the specified OpenAI model. For example, `extract_json('What is AI?', 'gpt-6-luna')` returns any relevant JSON data for the prompt. | `IMPORT openai.extract_json;` | Set OPENAI_API_KEY environment variable |
| `extract_json(String prompt, String model_name, Double temperature)` | Extracts JSON data from the given prompt using the specified OpenAI model and a specified temperature. With GPT-6 Luna, use the overload that also accepts a JSON schema and reasoningEffort, and set reasoningEffort to `none`. | `IMPORT openai.extract_json;` | Set OPENAI_API_KEY environment variable |
| `extract_json(String prompt, String model_name, Double temperature, Double topP)` | Extracts JSON data from the given prompt using the specified OpenAI model, with a specified temperature and top-p value. With GPT-6 Luna, use the overload that also accepts a JSON schema and reasoningEffort, and set reasoningEffort to `none`. | `IMPORT openai.extract_json;` | Set OPENAI_API_KEY environment variable |
| `extract_json(String prompt, String model_name, Double temperature, Double topP, String jsonSchema, String reasoningEffort)` | Extracts JSON data with an optional JSON schema and reasoning-effort setting. For models that support custom sampling in non-reasoning mode, set reasoningEffort to `none`. | `IMPORT openai.extract_json;` | Set OPENAI_API_KEY environment variable |
| `vector_embed(String text, String model_name)` | Embeds the given text into a vector using the specified OpenAI model. For example, `vector_embed('What is AI?', 'text-embedding-ada-002')` returns a vector representation of the text. | `IMPORT openai.vector_embed;` | Set OPENAI_API_KEY environment variable |
