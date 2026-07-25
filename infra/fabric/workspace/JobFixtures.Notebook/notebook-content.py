# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************

mode = "success"
marker = "default"
delay_seconds = 600

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import time

if mode == "failure":
    raise RuntimeError("FABRICQUERYR_INTENTIONAL_JOB_FAILURE")

if mode == "slow":
    time.sleep(int(delay_seconds))

mssparkutils.notebook.exit(f"fabricqueryr-job-success:{marker}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
