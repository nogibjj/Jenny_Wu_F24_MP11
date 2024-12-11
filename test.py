# test_notebooks

# Run the query, transform, and extract notebooks
from query import query
from extract import extract
from transform import transform

import unittest
from pyspark.sql import SparkSession

class TestNotebooks(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.getOrCreate()

    def test_query_function(self):
        # Test the query function
        result_df = query("""
            SELECT *
            FROM ids706_data_engineering.jcw131_nyc_shooting.jcw131_nyc_shooting_transformed
            WHERE Occur_Date = '04/19/2008'
        """, delta_table_name="ids706_data_engineering.jcw131_nyc_shooting.jcw131_nyc_shooting_transformed")
        
        self.assertIsNotNone(result_df, "The result DataFrame should not be None")
        self.assertGreater(result_df.count(), 0, "The result DataFrame should have at least one row")

    def test_transform_function(self):
        # Test the transform function
        input_df = self.spark.createDataFrame([
            (1, "04/19/2008", "Shooting"),
            (2, "04/20/2008", "Robbery")
        ], ["id", "Occur_Date", "Incident_Type"])
        
        transformed_df = transform(input_df)
        
        self.assertIsNotNone(transformed_df, "The transformed DataFrame should not be None")
        self.assertEqual(transformed_df.count(), 2, "The transformed DataFrame should have 2 rows")

    def test_extract_function(self):
        # Test the extract function
        extracted_df = extract("ids706_data_engineering.jcw131_nyc_shooting.jcw131_nyc_shooting_transformed")
        
        self.assertIsNotNone(extracted_df, "The extracted DataFrame should not be None")
        self.assertGreater(extracted_df.count(), 0, "The extracted DataFrame should have at least one row")

if __name__ == "__main__":
    unittest.main(argv=['first-arg-is-ignored'], exit=False)