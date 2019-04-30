#!/bin/bash

gcloud dataproc jobs submit pyspark ../code/MetricsWithSpark.py --cluster=turing --properties "spark.pyspark.python=python3,spark.pyspark.driver.python=python3"