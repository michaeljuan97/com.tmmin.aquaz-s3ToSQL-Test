import awswrangler as wr
import pandas as pd
from sqlalchemy import create_engine
import sys
import time
from datetime import datetime, timedelta
import logging

# # AWS S3 and database configuration
# if len(sys.argv) < 5:
#     print(f"args: f{sys.argv}")
#     print("Usage: python main.py <S3_BUCKET> <S3_KEY> <DATABASE_URI> <TABLE_NAME> <INTERVAL>")
#     sys.exit(1)

# S3_BUCKET = sys.argv[1]
# S3_KEY = sys.argv[2]
# DATABASE_URI = sys.argv[3]
# TABLE_NAME = sys.argv[4]
# INTERVAL = int(sys.argv[5])

# def load_parquet_from_s3(bucket: str, key: str) -> pd.DataFrame:
#     """
#     Load Parquet data from an S3 bucket into a Pandas DataFrame.
    
#     :param bucket: S3 bucket name
#     :param key: S3 object key
#     :return: DataFrame containing the parquet data
#     """
#     s3_path = f"s3://{bucket}/{key}"
#     print(f"Loading data from {s3_path}")
#     df = wr.s3.read_parquet(s3_path)
#     return df

# def insert_data_into_postgresql(df: pd.DataFrame, db_uri: str, table_name: str):
#     """
#     Insert DataFrame data into a PostgreSQL table.
    
#     :param df: DataFrame containing the data to insert
#     :param db_uri: Database URI for PostgreSQL
#     :param table_name: Name of the table to insert data into
#     """
#     engine = create_engine(db_uri)
    
#     with engine.connect() as connection:
#         print(f"Inserting data into {table_name} table...")
#         df.to_sql(table_name, connection, if_exists="append", index=False)
#         print("Data insertion complete.")

# def main():
#     # Load data from S3
#     df = load_parquet_from_s3(S3_BUCKET, S3_KEY)
    
#     # Insert data into PostgreSQL
#     # table_name = "randomName2"  # Set your target PostgreSQL table name here
#     insert_data_into_postgresql(df, DATABASE_URI, TABLE_NAME)

# if __name__ == "__main__":
#     # main()
#     while 1:
#         print("Job started")
#         logging.info(f"Job Started")
#         # print(S3_BUCKET)
#         # print(S3_KEY)
#         main()
#         print("Last Job run: ", datetime.now())
#         logging.info(f"Last Job Run", datetime.now())

#         print("Next job run: ", datetime.now()+ timedelta(seconds=INTERVAL))
#         logging.info(f"Next Job Run", datetime.now()+ timedelta(seconds=INTERVAL))
#         print("Going to sleep for ", INTERVAL, " seconds")
#         logging.info(f"Going to sleep for ", INTERVAL, " seconds")

#         time.sleep(INTERVAL)

# All configurations
# if len(sys.argv) < 5:
#     print(f"args: f{sys.argv}")
#     print("Usage: python main.py <DATABASE_URI> <CLASSIFICATION_PATH> <TIMESERIES_PATH> <HISTORICAL_PATH> <INTERVAL>")
#     sys.exit(1)

DATABASE_URI = "postgresql://test:test@sg.dd-tech.id:5432/postgres"
CLASSIFICATION_PATH = "s3://an021-aquaz-project/gold/classification/prediction-result/"
TIMESERIES_PATH = "s3://an021-aquaz-project/gold/forecast/result/result_per_shift/" #Ini bisa dihapus, bisa diambil dr classification. Jadiin nama table ML_result aja
HISTORICAL_PATH = "s3://an021-aquaz-project/silver/traceability/dbo/d_castting_historical/"
FEATURE_IMPORTANCE_PATH = "s3://an021-aquaz-project/gold/classification/feature-importance/"
INFERENCE_SUMMARY_TIMESERIES_PATH = "s3://an021-aquaz-project/gold/forecast/inference_summary/inference_summary_per_shift/"
INFERENCE_SUMMARY_CLASSIFICATION_PATH = "s3://an021-aquaz-project/gold/classification/inference-summary/"
INTERVAL = int(120)
# Todo: New func 2 new path. New csvs. Reset csvs. Test 1 cycle run. Then make gg component

S3_INSERTED_LIST = "s3://an021-aquaz-project/bronze/greengrass/inserted_list.csv" # NOT USED
S3_CLASSIFICATION_INSERTED_PATH = "s3://an021-aquaz-project/bronze/greengrass/classification_inserted_list.csv"
S3_TIMESERIES_INSERTED_PATH = "s3://an021-aquaz-project/bronze/greengrass/timeseries_inserted_list.csv"
S3_HISTORICAL_INSERTED_PATH = "s3://an021-aquaz-project/bronze/greengrass/historical_inserted_list.csv"
S3_FEATURE_IMPORTANCE_INSERTED_PATH = "s3://an021-aquaz-project/bronze/greengrass/feature_importance_inserted_list.csv"

S3_INFERENCE_SUMMARY_CLASSIFICATION_INSERTED_PATH = "s3://an021-aquaz-project/bronze/greengrass/inference_summary_classification_inserted_list.csv"
S3_INFERENCE_SUMMARY_TIMESERIES_INSERTED_PATH = "s3://an021-aquaz-project/bronze/greengrass/inference_summary_timeseries_inserted_list.csv"
# DATABASE_URI = sys.argv[1]
# CLASSIFICATION_PATH = sys.argv[2]
# TIMESERIES_PATH = sys.argv[3]
# HISTORICAL_PATH = sys.argv[4]
# INTERVAL = int(sys.argv[5])

timeseries_list = []
timeseries_inserted_list = []
temp_timeseries_inserted_list = []

classification_list = []
classification_inserted_list = []
temp_classification_inserted_list = []

historical_list = []
historical_inserted_list = []
temp_historical_inserted_list = []

feature_importance_list = []
feature_importance_inserted_list = []
temp_feature_importance_inserted_list = []

inference_summary_classification_list = []
inference_summary_classification_inserted_list = []
temp_inference_summary_classification_inserted_list = []

inference_summary_timeseries_list = []
inference_summary_timeseries_inserted_list = []
temp_inference_summary_timeseries_inserted_list = []

def load_parquet_from_s3(item: str) -> pd.DataFrame:
    """
    Load Parquet data from an S3 bucket into a Pandas DataFrame.

    :return: DataFrame containing the parquet data
    """
    s3_path = item
    print(f"Loading data from {s3_path}")
    data_tipe = item.split(".")
    try:
        if data_tipe[1] == "csv":
        # df = wr.s3.read_parquet(s3_path)
            df = wr.s3.read_csv(s3_path)
        else:
            df = wr.s3.read_parquet(s3_path)
        return df
    except:
        print("Invalid data")
        print(s3_path)

def insert_data_into_postgresql(df: pd.DataFrame, db_uri: str, table_name: str):
    """
    Insert DataFrame data into a PostgreSQL table.
    
    :param df: DataFrame containing the data to insert
    :param db_uri: Database URI for PostgreSQL
    :param table_name: Name of the table to insert data into
    """
    engine = create_engine(db_uri)
    
    with engine.connect() as connection:
        print(f"Inserting data into {table_name} table...")
        df.to_sql(table_name, connection, if_exists="append", index=False)
        print("Data insertion complete.")


def timeseries_job():
    timeseries = wr.s3.list_objects(TIMESERIES_PATH)
    # print(timeseries)
    for item in timeseries:
        print(item)
        timeseries_list.append(item)

    # filter inserted
    filtered_list2 = [item for item in timeseries_list if item not in timeseries_inserted_list]
    # print(timeseries_inserted_list)
    # print(timeseries_list)
    print(filtered_list2)

    for item in filtered_list2:
        df = load_parquet_from_s3(item)
        try:
            insert_data_into_postgresql(df, DATABASE_URI, "ML_RESULTS")
            # Update list inserted
            timeseries_inserted_list.append(item)
            # Update temp list
            temp_timeseries_inserted_list.append(item)
        except:
            print("Skipping data, invalid data")

def classification_job():
    classification = wr.s3.list_objects(CLASSIFICATION_PATH)
    # print(classification)
    for item in classification:
        print(item)
        classification_list.append(item)

    # filter inserted
    filtered_list2 = [item for item in classification_list if item not in classification_inserted_list]
    # print(classification_inserted_list)
    # print(classification_list)
    print(filtered_list2)

    for item in filtered_list2:
        df = load_parquet_from_s3(item)
        try:
            insert_data_into_postgresql(df, DATABASE_URI, "classification")
            # Update list inserted
            classification_inserted_list.append(item)
            # Update temp list
            temp_classification_inserted_list.append(item)
        except:
            print("Skipping data, invalid data")

def historical_job():
    historical = wr.s3.list_objects(HISTORICAL_PATH)
    # print(historical)
    for item in historical:
        print(item)
        historical_list.append(item)

    # filter inserted
    filtered_list2 = [item for item in historical_list if item not in historical_inserted_list]
    # print(historical_inserted_list)
    # print(historical_list)
    print(filtered_list2)

    for item in filtered_list2:
        df = load_parquet_from_s3(item)
        try:
            insert_data_into_postgresql(df, DATABASE_URI, "historical")
            # Update list inserted
            historical_inserted_list.append(item)
            # Update temp list
            temp_historical_inserted_list.append(item)
        except:
            print("Skipping data, invalid data")

def feature_importance_job():
    feature_importance = wr.s3.list_objects(FEATURE_IMPORTANCE_PATH)
    print(feature_importance)
    for item in feature_importance:
        print(item)
        feature_importance_list.append(item)

    # filter inserted
    filtered_list2 = [item for item in feature_importance_list if item not in feature_importance_inserted_list]
    # print(feature_importance_inserted_list)
    # print(feature_importance_list)
    print(filtered_list2)

    for item in filtered_list2:
        df = load_parquet_from_s3(item)
        try:
            insert_data_into_postgresql(df, DATABASE_URI, "feature_importance")
            # Update list inserted
            # historical_inserted_list.append(item)
            feature_importance_inserted_list.append(item)
            # Update temp list
            temp_feature_importance_inserted_list.append(item)
        except:
            print("Skipping data, invalid data")

# Untested
def inference_summary_classification_job():
    inference_summary_classification = wr.s3.list_objects(INFERENCE_SUMMARY_CLASSIFICATION_PATH)
    print(inference_summary_classification)
    for item in inference_summary_classification:
        print(item)
        inference_summary_classification_list.append(item)

    # filter inserted
    filtered_list2 = [item for item in inference_summary_classification_list if item not in inference_summary_classification_inserted_list]
    # print(inference_summary_classification_inserted_list)
    # print(inference_summary_classification_list)
    print(filtered_list2)

    for item in filtered_list2:
        df = load_parquet_from_s3(item)
        try:
            insert_data_into_postgresql(df, DATABASE_URI, "inference_summary_classification")
            # Update list inserted
            # historical_inserted_list.append(item)
            inference_summary_classification_inserted_list.append(item)
            # Update temp list
            temp_inference_summary_classification_inserted_list.append(item)
        except:
            print("Skipping data, invalid data")

def inference_summary_timeseries_job():
    inference_summary_timeseries = wr.s3.list_objects(INFERENCE_SUMMARY_TIMESERIES_PATH)
    print(inference_summary_timeseries)
    for item in inference_summary_timeseries:
        print(item)
        inference_summary_timeseries_list.append(item)

    # filter inserted
    filtered_list2 = [item for item in inference_summary_timeseries_list if item not in inference_summary_timeseries_inserted_list]
    # print(inference_summary_timeseries_inserted_list)
    # print(inference_summary_timeseries_list)
    print(filtered_list2)

    for item in filtered_list2:
        df = load_parquet_from_s3(item)
        try:
            insert_data_into_postgresql(df, DATABASE_URI, "inference_summary_timeseries")
            # Update list inserted
            inference_summary_timeseries_list.append(item)
            # Update temp list
            temp_inference_summary_timeseries_list.append(item)
        # except:
        #     print("Skipping data, invalid data")
        except Exception as e:
            print(f"Skipping data, invalid data: {e}")
            # return None
# End of untested

def append_to_csv_in_s3(s3_path, new_data):
    """
    Append data to an existing CSV file in S3.

    Parameters:
        s3_path (str): S3 path to the existing CSV file.
        new_data (pd.DataFrame): DataFrame containing new rows to append.

    Returns:
        None
    """
    try:
        # Read the existing data from the S3 CSV file
        print(f"Reading existing data from {s3_path}...")
        existing_data = wr.s3.read_csv(s3_path)
        print(f"Existing data loaded. Rows: {len(existing_data)}")

        # Append the new data to the existing data
        combined_data = pd.concat([existing_data, new_data], ignore_index=True)
        print(f"Appended new data. Total Rows: {len(combined_data)}")

        # Write the combined data back to S3
        print(f"Writing updated data back to {s3_path}...")
        wr.s3.to_csv(combined_data, path=s3_path, index=False)
        print("Data successfully appended to the CSV file in S3.")

    except Exception as e:
        print(f"An error occurred: {e}")

def read_inserted_list_from_s3(s3_path):
    """
    Read a CSV file from S3 and extract values from 'column1' into a Python list.

    Parameters:
        s3_path (str): S3 path to the CSV file.

    Returns:
        list: List of values from 'column1' of the CSV file.
    """
    try:
        # Read the CSV file into a Pandas DataFrame
        print(f"Reading CSV file from {s3_path}...")
        df = wr.s3.read_csv(s3_path)

        header = df.columns[0]

        if header == "timeseries_inserted_list":

            # Extract values from 'column1' and convert to a list
            column1_values = df['timeseries_inserted_list'].tolist()
            print(f"Extracted 'column1' values. Total values: {len(column1_values)}")
            
            for x in column1_values:
                timeseries_inserted_list.append(x)
        elif header == "classification_inserted_list":
            # Extract values from 'column1' and convert to a list
            column1_values = df['classification_inserted_list'].tolist()
            print(f"Extracted 'column1' values. Total values: {len(column1_values)}")
            
            for x in column1_values:
                classification_inserted_list.append(x)
        elif header == "historical_inserted_list":
            # Extract values from 'column1' and convert to a list
            column1_values = df['historical_inserted_list'].tolist()
            print(f"Extracted 'column1' values. Total values: {len(column1_values)}")
            
            for x in column1_values:
                historical_inserted_list.append(x)
        elif header == "feature_importance_inserted_list":
            # Extract values from 'column1' and convert to a list
            column1_values = df['feature_importance_inserted_list'].tolist()
            print(f"Extracted 'column1' values. Total values: {len(column1_values)}")
            
            for x in column1_values:
                feature_importance_inserted_list.append(x)

        elif header == "inference_summary_classification_inserted_list":
            # Extract values from 'column1' and convert to a list
            column1_values = df['inference_summary_classification_inserted_list'].tolist()
            print(f"Extracted 'column1' values. Total values: {len(column1_values)}")
            
            for x in column1_values:
                inference_summary_classification_inserted_list.append(x)

        elif header == "inference_summary_timeseries_inserted_list":
            # Extract values from 'column1' and convert to a list
            column1_values = df['inference_summary_timeseries_inserted_list'].tolist()
            print(f"Extracted 'column1' values. Total values: {len(column1_values)}")
            
            for x in column1_values:
                inference_summary_timeseries_inserted_list.append(x)


        return None
    except Exception as e:
        print(f"An error occurred while reading the CSV file: {e}")
        return None

if __name__ == "__main__":
    # Run only on startup
    # read_inserted_list_from_s3(S3_INSERTED_LIST)
    # read_inserted_list_from_s3(S3_CLASSIFICATION_INSERTED_PATH)
    # read_inserted_list_from_s3(S3_TIMESERIES_INSERTED_PATH)
    # read_inserted_list_from_s3(S3_HISTORICAL_INSERTED_PATH)
    # read_inserted_list_from_s3(S3_FEATURE_IMPORTANCE_INSERTED_PATH)
    # read_inserted_list_from_s3(S3_INFERENCE_SUMMARY_CLASSIFICATION_INSERTED_PATH)
    read_inserted_list_from_s3(S3_INFERENCE_SUMMARY_TIMESERIES_INSERTED_PATH)
    # print(historical_inserted_list)
    # print(classification_inserted_list)
    # print(timeseries_inserted_list)
    # print(feature_importance_inserted_list)
    # print(inference_summary_classification_inserted_list)
    print(inference_summary_timeseries_inserted_list)
    
    while 1:
        # timeseries_job()
        # classification_job()
        # historical_job()
        # feature_importance_job()

        # inference_summary_classification_job()
        inference_summary_timeseries_job()

        # New data to append
        # new_rows_timeseries = pd.DataFrame({
        #     "timeseries_inserted_list": temp_timeseries_inserted_list
        #     })

        # # New data to append
        # new_rows_classification = pd.DataFrame({
        #     "classification_inserted_list": temp_classification_inserted_list
        #     })

        #         # New data to append
        # new_rows_historical = pd.DataFrame({
        #     "historical_inserted_list": temp_historical_inserted_list
        #     })

        # # New data to append
        # new_rows_feature_importance = pd.DataFrame({
        #     "feature_importance_inserted_list": temp_feature_importance_inserted_list
        #     })

        #         # New data to append
        # new_rows_inference_summary_classification = pd.DataFrame({
        #     "inference_summary_classification_inserted_list": temp_inference_summary_classification_inserted_list
        #     })

                # New data to append
        new_rows_inference_summary_timeseries = pd.DataFrame({
            "inference_summary_timeseries_inserted_list": temp_inference_summary_timeseries_inserted_list
            })

        # Append data to the S3 CSV file
        # append_to_csv_in_s3(S3_TIMESERIES_INSERTED_PATH, new_rows_timeseries)
        # append_to_csv_in_s3(S3_CLASSIFICATION_INSERTED_PATH, new_rows_classification)
        # append_to_csv_in_s3(S3_HISTORICAL_INSERTED_PATH, new_rows_historical)
        # append_to_csv_in_s3(S3_FEATURE_IMPORTANCE_INSERTED_PATH, new_rows_feature_importance)

        # append_to_csv_in_s3(S3_INFERENCE_SUMMARY_CLASSIFICATION_INSERTED_PATH, new_rows_inference_summary_classification)
        append_to_csv_in_s3(S3_INFERENCE_SUMMARY_TIMESERIES_INSERTED_PATH, new_rows_inference_summary_timeseries)

        # Clear temp lists
        # temp_timeseries_inserted_list.clear()
        # temp_classification_inserted_list.clear()
        # temp_historical_inserted_list.clear()
        # temp_feature_importance_inserted_list.clear()
        # temp_inference_summary_classification_inserted_list.clear()
        temp_inference_summary_timeseries_inserted_list.clear()

        print("Last Job run: ", datetime.now())
        print("Next job run: ", datetime.now()+ timedelta(seconds=INTERVAL))
        print("Going to sleep for ", INTERVAL, " seconds")
        time.sleep(INTERVAL)