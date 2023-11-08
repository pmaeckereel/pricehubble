## Code used for ETL parameters retrieval.

from datetime import datetime
from typing import Dict, Optional

def lambda_handler(event: Optional[Dict], context: Optional[Dict]) -> Dict:
	"""
	Function called by default by Lambda service.

	Parameters:
	
		event (Dict): a key-value dict sent to the lambda.
		context (Dict): additional 
	"""
	if "date" in event.keys():
		date = event["date"]
	else:
		date = datetime.now().strftime("%Y-%m-%d")

	## Other parameters check is done here. Types and formats of parameters can be ensured too.

	return {
        'statusCode': 200,
        'body': {'date': date}
    }