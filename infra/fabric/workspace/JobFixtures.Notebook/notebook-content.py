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
import json

if mode == "failure":
    raise RuntimeError("FABRICQUERYR_INTENTIONAL_JOB_FAILURE")

if mode == "slow":
    time.sleep(int(delay_seconds))

if mode == "configuration":
    import notebookutils

    notebookutils.notebook.exit(json.dumps({
        "marker": marker,
        "shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions"),
        "environment_broadcast_timeout": spark.conf.get("spark.sql.broadcastTimeout"),
        "lakehouse_id": notebookutils.runtime.context["defaultLakehouseId"],
        "row_count": spark.table("dbo.fabricqueryr_basic").count(),
    }))

mssparkutils.notebook.exit(f"fabricqueryr-job-success:{marker}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
