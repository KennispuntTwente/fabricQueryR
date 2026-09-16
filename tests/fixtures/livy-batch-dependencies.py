"""Assert Livy batch dependencies inside Spark and write an independent marker."""

import json
import sys
import traceback
from pathlib import Path

import notebookutils
from pyspark import SparkFiles
from pyspark.sql import SparkSession

def read_archive(_):
    from pathlib import Path
    from pyspark import SparkFiles

    return Path(SparkFiles.get("archive.zip"), "marker.txt").read_text()


def read_file(_):
    from pathlib import Path
    from pyspark import SparkFiles

    return Path(SparkFiles.get("plain.txt")).read_text()


def main():
    import fabricqueryr_dependency as dependency

    spark = SparkSession.builder.getOrCreate()
    expected = "fabricqueryr-dependency-73"
    assert dependency.VALUE == expected, "py_files module missing"
    assert spark.sparkContext.parallelize([1], 1).map(read_file).collect() == [expected], "files content mismatch"
    assert spark.sparkContext.parallelize([1], 1).map(read_archive).collect() == [expected], "archives content mismatch"
    resource = (
        spark._jvm.java.lang.Thread.currentThread()
        .getContextClassLoader()
        .getResourceAsStream("fabricqueryr-resource.txt")
    )
    assert resource is not None, "jars resource missing"
    assert spark._jvm.java.util.Scanner(resource).useDelimiter("\\A").next() == expected
    # The first setting comes only from the published TestEnvironment fixture.
    assert spark.conf.get("spark.sql.broadcastTimeout") == "301", "environment config missing"
    assert spark.conf.get("spark.sql.shuffle.partitions") == "3", "conf override missing"
    return {"marker": sys.argv[2], "dependencies": expected}


try:
    result = main()
except Exception:
    notebookutils.fs.put(sys.argv[1], json.dumps({"error": traceback.format_exc()}), True)
    raise
else:
    notebookutils.fs.put(sys.argv[1], json.dumps(result), True)
