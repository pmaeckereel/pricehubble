## Main file, used to process input dataset 'sample.json' into a clean dataset.
## The sample is supposed to be stored on S3, and the cleaned data will be stored on a Glue database.
import sys
import boto3

from data_loading import DataLoader

class DataLoaderSample(DataLoader):
	"""
	Class used to load data from an S3 datalake.
	"""
	def __init__(self, region: str):
		"""
		Set clients as attributes to be able to reuse them if needed.
		"""
		self.s3_client = boto3.client('s3', region=region)
		self.athena_client = boto3.client('athena', region=region)

	def load_data(self, bucket: str, path: str) -> pd.DataFrame:
		"""
		Load the data corresponding to all files at given path.

		Parameters:

			bucket(str): the S3 bucket to load data from.
			path (str): the path to load data from. Can be a 'folder' path or a 'file' path.

		Returns:

			pd.DataFrame: a dataframe with all cleaned datapoints.
		"""
		## Check if path is a folder or a file. If it's a folder, recursive loop inside.
		## If it's a file, call load_one_file() method.
		## Concat all not-None dataframes into a single one and return it.

	def load_one_file(self, bucket: str, filepath: str) -> pd.DataFrame:
		"""
		Load data from one file stored on S3.
		Data is here supposed to be stored as json, under the same format as the 'sample.json'.

		Parameters:

			bucket(str): the S3 bucket to load data from.
			path (str): the path to a file to load data from.

		Returns:
			pd.DataFrame: a cleaned dataframe with expected fields and types. 
		"""
		## Load data from S3 with boto3.
		## Call clean_data() method.
		## Return a pd.DataFrame from the cleaned Dict if it's not None.

	def clean_data(self, raw_data: Dict) -> Dict:
		"""
		Clean the data. Remove unused fields, ensure presence, type and format of expected fields.
		All expectations are specified here, but could be in a separated file if there are too many 
		and it becomes messy.

		Parameters:

			raw_data (Dict): represents one datapoint.

		Returns:

			pd.DataFrame: a dataframe representing a cleaned datapoint.
		"""
		## Defining expectations.
		expected_types = {
			"id": "str",
			"living_area": "float",
			"property_type": "str",
			"scraping_date": "str"
		}
		expected_date_format = "YYYY-MM-DD"
		expected_property_type_enum = ["house", "appartment"]
		living_area_range = [10, 500]
		price_per_square_meter_range = [500, 15000]

		## Check that all keys in 'expected_types' are in 'raw_data.keys()'. Raise error if one is not there.
		## Keep only keys defined in 'expected_types'.
		## Check that values in 'raw_data'.values() are instanceof specified in expected_types. Cast if possible.
		## Check that 'scraping_date' is under the right format. Cast if possible.
		## !!! Here I prefer to cast not valid date format rather than just discard the whole point. 
		## If there is a reason to really discard it, then it can be done here.

		## Check that 'property_type' has only values defined in the enum 'expected_property_type_enum'. Don't keep if not.
		## Check that 'living_area' value is between 'living_area_range'.
		## Use 'cast_price' method to cast 'raw_price' value to float. Assign result to 'price' key if not null.
		## Get 'price_per_square_meter_range' from 'price' and 'living_area'. Keep if result in 'price_per_square_meter_range'.

		## Return the Dict if everything went well, None otherwise.

	def cast_price(price: str) -> float:
		"""
		Method used to cast price, given as str, to a float.

		Parameters:

			price(str): the string representing the price.

		Returns:
			float: the price extracted from the string.
		"""
		## Use preprocessing methods, like lower or strip to get extract only the price.
		## If all prices are given in monthly basis, regex can be used to extract numbers from the string.
		## Otherwise, time basis (monthly, yearly...) must be extracted from the string too.
		## If extraction fails/is not possible, return None.

	def store_cleaned_data(self, df: pd.DataFrame, table_name="sample":str) -> None: 
		"""
		Load the cleaned data to a glue/athena table.

		Parameters:

			df (pd.DataFrame): a dataframe containing all datapoints cleaned.

		Returns:

			None
		"""
		## Use self.athena_client to save dataframe to a glue/athena table named 'table_name'.
		## Depending on the chosen partition system, other parameters like date might be required.

	def main(self, bucket, input_path, output_table):
		df = self.load_data(bucket, input_path)
		self.store_cleaned_data(df, table_name)

if __name__ == "__main__":
	## Ensure args.
	if len(sys.argv) != 5:
		logging.error("Error! 4 args required: the input bucket name and datalake path, the athena table name "
			f"and the aws region. Got {sys.argv} instead. Exiting...")
		sys.exit(1)

	bucket = sys.argv[1]
	path = sys.argv[2]
	table = sys.argv[3]
	region = sys.argv[4]

	data_loader_sample = DataLoaderSample(region)
	data_loader_sample.main(bucket, path, table)