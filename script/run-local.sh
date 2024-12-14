$SPARK_HOME/bin/spark-submit \
--py-files packages.zip \
dependencies/job_submitter.py --job pipeline --conf-file configs/config.json