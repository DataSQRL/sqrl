from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes
from pyflink.table.udf import udf

# Set up the Flink Table environment
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

# Define UDFs for sine and cosine
@udf(result_type=DataTypes.DOUBLE())
def py_sin(x: float):
  import math
  return math.sin(x)

@udf(result_type=DataTypes.DOUBLE())
def py_cos(x: float):
  import math
  return math.cos(x)