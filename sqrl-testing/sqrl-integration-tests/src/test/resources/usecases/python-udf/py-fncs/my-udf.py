from pyflink.table import DataTypes
from pyflink.table.udf import ScalarFunction, udf
import time

class SumTwoNumbersUDF(ScalarFunction):
    def open(self, function_context):
        """
        No initialization needed here, but this method is required
        by the ScalarFunction interface.
        """
        pass

    def eval(self, num1: float, num2: float) -> float:
        """
        Returns the sum of two float values.

        Args:
            num1 (float): The first number.
            num2 (float): The second number.

        Returns:
            float: The sum of num1 and num2.
        """
        return num1 + num2

# Instantiate and wrap the UDF
sum_two_numbers_instance = SumTwoNumbersUDF()
sum_two_numbers_udf = udf(sum_two_numbers_instance, result_type=DataTypes.FLOAT())

# Benchmarking code
if __name__ == "__main__":
    # Initialize the UDF (simulate the Flink environment by passing None)
    sum_two_numbers_instance.open(None)

    # Sample numbers
    num1 = 3.5
    num2 = 2.1

    # Number of times to run eval
    num_runs = 10

    # Warm-up run
    print("Running warm-up eval...")
    sum_two_numbers_instance.eval(num1, num2)

    # Start the timer
    print(f"Running eval {num_runs} times...")
    start_time = time.time()

    # Run eval multiple times
    for i in range(num_runs):
        result = sum_two_numbers_instance.eval(num1, num2)
        print(f"Run {i+1}: {num1} + {num2} = {result}")

    # End the timer
    end_time = time.time()

    # Calculate elapsed time in milliseconds
    elapsed_time_ms = (end_time - start_time) * 1000
    average_time_ms = elapsed_time_ms / num_runs

    print(f"\nBenchmark Results:")
    print(f"Total time for {num_runs} evals: {elapsed_time_ms:.2f} ms")
    print(f"Average time per eval: {average_time_ms:.2f} ms")
