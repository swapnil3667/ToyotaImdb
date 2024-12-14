import unittest
from dependencies.job_submitter import create_spark_session, load_config_file
from jobs.pipeline import transform_top_movies


class SparkETLTests(unittest.TestCase):
    """Test suite for transformation in job/pipeline.py
    """

    def setUp(self):
        """Start Spark, define config and path to test data
        """
        self.config =   load_config_file("configs/config.json")
        self.spark, *_ = create_spark_session("test_job")
        self.test_data_path = 'tests/test_data/'

    def tearDown(self):
        """Stop Spark
        """
        self.spark.stop()

    def read_tsv(self, filepath: str):
        df = (self.spark.read
              .option("header", "true")
              .option("sep", "\t")
              .csv(filepath))
        return df

    def test_transform_data(self):
        title_basic_df = self.read_tsv(self.config["title_basic"])
        title_rating_df = self.read_tsv(self.config["title_rating"])
        result_df = transform_top_movies(title_basic_df, title_rating_df,self.config)
        self.assertEqual(10, result_df.count())

if __name__ == '__main__':
    unittest.main()