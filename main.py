from data_setup.components.database_config import FinancialDatabase, setup_database
from data_setup.components.data_collection_config import FinancialDataCollector
import os
from dotenv import load_dotenv

load_dotenv()
alpha_vantage_key = os.getenv('ALPHA_VANTAGE_API_KEY')
fred_api_key=os.getenv('FRED_API_KEY')

## Setup Database
if not os.path.exists('database_and_schema/financial_markets.db'):
    db = setup_database()
else:
    db = FinancialDatabase(db_path="database_and_schema/financial_markets.db")
db.get_data_summary()

#Collect Data
data_collector = FinancialDataCollector(alpha_vantage_key=alpha_vantage_key, fred_api_key=fred_api_key)
overview = data_collector.get_asset_overview_alpha_vantage('AXON')

#Store Data
db.insert_asset(overview)
db.get_data_summary()
db.get_asset_overview('AXON')
db.close()

'''
from data_setup.pipelines.data_collection_pipeline import FinancialDataPipeline
pipeline = FinancialDataPipeline(
    
)

pipeline.setup_database()
pipeline.run_quick_test()
'''