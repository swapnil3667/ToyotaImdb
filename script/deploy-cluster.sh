spark-submit --py-files packages.zip \
--master <Master URL> \
--num-executors 2 \
--driver-memory 1G \
--executor-memory 2G \
--executor-cores 4 \
dependencies/job_submitter.py --job pipeline --conf-file configs/config.json