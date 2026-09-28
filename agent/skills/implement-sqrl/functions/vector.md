# Vector Functions

| Function | Description | Example |
|----------|-------------|---------|
| `cosine_similarity` | Computes cosine similarity between two vectors. | `cosine_similarity(vec1, vec2)` |
| `cosine_distance` | Computes cosine distance between two vectors (1 - cosine similarity). | `cosine_distance(vec1, vec2)` |
| `euclidean_distance` | Computes the Euclidean distance between two vectors. | `euclidean_distance(vec1, vec2)` |
| `double_to_vector` | Converts a `DOUBLE[]` array into a `VECTOR`. | `double_to_vector([1.0, 2.0, 3.0])` |
| `vector_to_double` | Converts a `VECTOR` into a `DOUBLE[]` array. | `vector_to_double(vec)` |
| `center` | Computes the centroid (average) of a collection of vectors. Aggregate function. | `SELECT center(vecCol) FROM vectors` |
