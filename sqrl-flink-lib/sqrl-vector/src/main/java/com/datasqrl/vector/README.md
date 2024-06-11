| Function Documentation |
|-------------------------|
| `AsciiTextTestEmbed(string) → vector`<br><br> Convert text to a vector of length 256 where each character's ASCII value is mapped. <br> Example: `AsciiTextTestEmbed('hello') → [0, 0, 0, ..., 1, 0, 1, 2, ...]` |
| `Center(vector) → vector`<br><br> Aggregate function to compute the center of multiple vectors. <br> Example: `Center([1.0, 2.0], [3.0, 4.0]) → [2.0, 3.0]` |
| `CosineDistance(vector, vector) → double` <br><br> Compute the cosine distance between two vectors. <br> Example: `CosineDistance([1.0, 0.0], [0.0, 1.0]) → 1.0` |
| `CosineSimilarity(vector, vector) → double` <br><br> Compute the cosine similarity between two vectors. <br> Example: `CosineSimilarity([1.0, 0.0], [0.0, 1.0]) → 0.0` |
| `DoubleToVector(array<double>) → vector` <br><br> Convert an array of doubles to a vector. <br> Example: `DoubleToVector([1.0, 2.0, 3.0]) → [1.0, 2.0, 3.0]` |
| `EuclideanDistance(vector, vector) → double` <br><br> Compute the Euclidean distance between two vectors. <br> Example: `EuclideanDistance([1.0, 0.0], [0.0, 1.0]) → 1.41421356237` |
| `OnnxEmbed(string, string) → vector` <br><br> Convert text to a vector using an ONNX model. <br> Example: `OnnxEmbed('hello', '/path/to/model') → [0.5, 0.1, ...]` |
| `VectorToDouble(vector) → array<double>` <br><br> Convert a vector to an array of doubles. <br> Example: `VectorToDouble([1.0, 2.0, 3.0]) → [1.0, 2.0, 3.0]` |