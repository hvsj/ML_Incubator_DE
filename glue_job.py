# Import required libraries
try:
    import os
    import sys
    import json
    import boto3
    from pathlib import Path, PurePosixPath
    from typing import Union, TypeVar

    from pyspark.sql.session import SparkSession

    from awsglue.transforms import *
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    from awsglue.job import Job
except exception as e:
    print("Unable to import modules: ", str(e))

# initialize global variables
AWS_PROFILE_NAME = "ml-inc-gp-profile"
S3_PATH_STRUCTURE: str = "s3://{bucket_name}/{file_name}"

BUCKET_NAME = "feedin-bkt"
SOURCE_FOLDER_NAME = "covid-19-testing-data/dataset/csv/"
TARGET_FOLDER_NAME = "covid-19-testing-data/dataset/parquet/"

# Create a session with the specified profile
SESSION = boto3.Session(profile_name=AWS_PROFILE_NAME)

# Create an S3 client using the session
S3_CLIENT = SESSION.client("s3")


def convert_csv_to_parquet(source_path: str, target_path: str):
    """
    This function will take a csv file as input and convert it to parquet file.
    :param source_path: source file path
    :param target_path: target file path
    :return: It returns either True or False regarding the status of conversion and
                its respective error message if it fails
    """
    try:
        # Read the CSV file
        print("Source Path: " + source_path)
        csv_datasource = glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={"paths": [source_path]},
            format="csv",
            format_options={"withHeader": True}
        )

        # Convert the CSV dynamic frame to a Spark DataFrame
        # csv_dataframe = csv_datasource.toDF()

        # Write the DataFrame in Parquet format
        write_parquet_out = glueContext.write_dynamic_frame.from_options(
            # frame=csv_dataframe,
            frame=csv_datasource,
            connection_type="s3",
            connection_options={"path": target_path, "partitionKeys": []},
            format="parquet",
            format_options={
                # "compression": "gzip"
                "useGlueParquetWriter": True
            },
            transformation_ctx="write_parquet_out",
        )

        # We can also use DataFrames in a script (pyspark.sql.DataFrame).
        # Write the DataFrame in Parquet format
        # csv_dataframe.write.parquet(TARGET_PATH, mode="overwrite")
    except Exception as e:
        print("Exception:")
        print(e)
        return False, "Unable to convert CSV file to PARQUET, please find detailed exception in exception message!!", str(
            e)
    else:
        print("Source Path: " + target_path)
        return True, "Succesfully converted CSV file to PARQUET", None


def get_list_of_csv_files(s3_client, bucket_name, sorce_folder_name, target_folder_name, s3_path_structure):
    try:
        dict_of_csv_files: list = {}

        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=sorce_folder_name
        )

        # Print the file names
        if "Contents" in response:
            for file in response["Contents"]:
                # ignoring folders
                if not file['Key'].endswith('/'):
                    print("*" * 20)

                    file_name = str(Path(file['Key']).name)
                    print("filename: " + file_name)

                    source_file_path = s3_path_structure.format(bucket_name=bucket_name, file_name=file['Key'])
                    print("Source filepath: " + source_file_path)

                    target_subfolder_name = str(PurePosixPath(file['Key']).parent.name)
                    target_path_name = f"{target_folder_name}{target_subfolder_name}/"
                    target_file_path = s3_path_structure.format(bucket_name=bucket_name, file_name=target_path_name)
                    print("Target filepath: " + target_file_path)

                    dict_of_csv_files[file_name] = {
                        "FILE_NAME": file_name,
                        "SOURCE_PATH": source_file_path,
                        "TARGET_PATH": target_file_path
                    }

                    print("*" * 20)

            return True, "Found some files in folder", dict_of_csv_files

        else:
            print("No files found in the folder.")
            return False, "No files found in the folder.", []

    except Exception as e:
        print("Exception:")
        print(e)
        raise Exception(e)


def main():
    # Create a Spark context
    # sc = SparkContext()
    # sc = spark.sparkContext
    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)

    spark = glueContext.spark_session

    # Get the job arguments
    # args = getResolvedOptions(sys.argv, ["JOB_NAME"])

    # Create a Glue job
    job = Job(glueContext)
    # job.init(args["JOB_NAME"], args)

    # need to iterate through all the csv files in csv folder and the convert them to parquet files

    fun_response, fun_status_msg, dict_of_csv_files = get_list_of_csv_files(s3_client=S3_CLIENT,
                                                                            bucket_name=BUCKET_NAME,
                                                                            sorce_folder_name=SOURCE_FOLDER_NAME,
                                                                            target_folder_name=TARGET_FOLDER_NAME,
                                                                            s3_path_structure=S3_PATH_STRUCTURE)

    if fun_status_msg is False:
        print(fun_status_msg)
        print()
        print("No files to convert into parqet files!!")
    else:
        print(dict_of_csv_files)

        for key, value in dict_of_csv_files.items():

            source_path = value["SOURCE_PATH"]
            target_path = value["TARGET_PATH"]
            fun_response, fun_status_msg, fun_exception_msg = convert_csv_to_parquet(source_path=source_path,
                                                                                     target_path=target_path)

            if fun_response is False:
                raise Exception(fun_exception_msg)

            print(fun_status_msg)

    # Commit the job
    job.commit()


if __name__ == "__main__":
    main()
