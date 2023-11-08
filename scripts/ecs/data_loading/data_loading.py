## File containing only an abstract class. 
## This class has the prototypes of methods that must be used by all data loaders.
## There is one data loader by data source.

import pandas as pd
from abc import ABC, abstractmethod

class DataLoader(ABC):
	"""
	Abstract class used to have a uniformed loading through all sources.
	"""

	def __init__(self, **kwargs):
		"""
		Any loader can have its own init method and parameters.
		"""
		pass

	@abstractmethod
	def load_data(self, **kwargs) -> pd.DataFrame:
		"""
		Several arguments can be passed, like a table name or a datalake path.
		Clean data before saving it to save RAM.
		"""
		## For each data loaded (a row in json/csv file, a line from a DB), clean it 
		pass

	@abstractmethod
	def clean_data(self, raw_data, **kwargs) -> pd.DataFrame:
		"""
		Clean the data. Remove unused fields, ensure type and format.

		Parameters:

			raw_data: represents one datapoint. Can be a Dict, a pd.DataFrame.

		Returns:

			pd.DataFrame: a dataframe representing a cleaned datapoint.
		"""
		pass

	@abstractmethod
	def store_cleaned_data(self, df: pd.DataFrame, **kwargs) -> None: 
		"""
		Load the cleaned data to another storage, like a datawarehouse or a database.
		"""
		pass

	@abstractmethod
	def main(self, **kwargs):
		df = self.load_data(kwargs)
		self.store_cleaned_data(df, kwargs)
