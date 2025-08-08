from data_setup.pipelines.data_collection_pipeline import FinancialDataPipeline
pipeline = FinancialDataPipeline(
    alpha_vantage_key="B1X0S6TCCVSR7VT4",
    fred_api_key="c0a90a9c195271e24840c9e8a3b72c6b"
)

pipeline.setup_database()
pipeline.run_quick_test()