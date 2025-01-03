import os
import zipfile

import boto3
import pandas as pd


def connect(bucket_name, region="us-west-2"):
    try:
        s3_client = boto3.client("s3", region_name=region)
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        download_dir = os.path.expanduser('~/Downloads/')
        os.makedirs(download_dir, exist_ok=True)
        if "Contents" in response:
            for object_name in response["Contents"]:
                file_name = object_name['Key']

                zip_file_path = os.path.join(download_dir, os.path.basename(file_name))
                s3_client.download_file(bucket_name, file_name, zip_file_path)
                with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
                    zip_ref.extractall(download_dir)
        else:
            print(f"The bucket '{bucket_name}' is empty or does not exist.")

    except Exception as e:
        print(f"An error occurred: {e}")


def operation_one(file_name,base_directory):

    file_path=os.path.join(base_directory,file_name)

    try:
        df = pd.read_csv(file_path)
        row = df[df['id'] == "MO_BS_INV"]
        column_name="2014-10-01"
        if not row.empty:
            if column_name in row.columns:
                value = row[column_name].values[0]
                return value
            else:
                return f"Column '{column_name}' not found in the data."

    except FileNotFoundError:
        print(f"The file {file_name} was not found at {base_directory}")

def operation_two(file_name,base_directory):
    file_path = os.path.join(base_directory, file_name)
    try:
        df = pd.read_csv(file_path)
        row = df[df['id'] == "MO_BS_AP"]
        numeric_values = row.drop(columns=['id', 'scale']).values[0]
        mean=numeric_values.mean()
        return mean

    except FileNotFoundError:
        print(f"The file {file_name} was not found at {base_directory}")

def operation_three(file_name,base_directory):
    file_path = os.path.join(base_directory, file_name)
    try:
        df = pd.read_csv(file_path)
        row = df[df['id'] == "MO_BS_Intangibles"]
        target_date="2015-09-30"
        date_columns = [col for col in df.columns if col not in ["scale", "id"]]
        matching_column = None
        for col in date_columns:
            if pd.to_datetime(col) <= pd.to_datetime(target_date):
                matching_column = col
            else:
                break
        if matching_column is None:
            print(f"No matching column found for {target_date} in {file_name}")
            return

        value = row[matching_column].values[0]
        return value
    except FileNotFoundError:
        print(f"The file {file_name} was not found at {base_directory}")

def operation_four(file_names, base_directory, row_name):
    values = []

    for file_name in file_names:
        file_path = os.path.join(base_directory, file_name)
        try:
            df = pd.read_csv(file_path)

            df.columns = df.columns.str.strip()

            row = df[df['id'] == row_name]

            if not row.empty:
                row_data = row.drop(columns=['id', 'scale'], errors='ignore')
                row_values = pd.to_numeric(row_data.values.flatten(), errors='coerce')
                values.extend(row_values)

            else:
                print(f"Row '{row_name}' not found in file '{file_name}'")

        except Exception as e:
            print(f"Error reading file {file_name}: {e}")

    if values:
        return sum(values) / len(values)

    else:
        print(f"No data found for row '{row_name}' across the files.")
        return None

def operation_six(file_names, base_directory, row_name):

    results = []
    threshold=20000000000

    for file_name in file_names:
        file_path = os.path.join(base_directory, file_name)
        try:
            df = pd.read_csv(file_path)
            row = df[df['id'] == row_name]

            if not row.empty:
                row_data = row.drop(columns=['id', 'scale'], errors='ignore')

                row_values = pd.to_numeric(row_data.values.flatten(), errors='coerce')

                row_data = row_data.apply(pd.to_numeric, errors='coerce')

                for col, value in zip(row_data.columns, row_values):
                    if pd.notna(value) and value > threshold:
                        results.append((col, value))
                    else:
                        if pd.isna(value):
                            print(f"NaN encountered for {row_name} at column {col}")
                        else:
                            print(f"Value {value} for {row_name} at column {col} is not greater than {threshold}")

            else:
                print(f"Row '{row_name}' not found in file '{file_name}'")

        except Exception as e:
            print(f"Error reading file {file_name}: {e}")
    return results

def main():
    bucket_name = "as-findata-tech-challenge"
    connect(bucket_name)

    base_directory = os.path.expanduser('~/Downloads/')
    result_1= operation_one("MNZIRS0108.csv",base_directory)
    print(f"Answer to Question 1: {result_1}")

    result_2=operation_two("Y1HZ7B0146.csv",base_directory)
    print(f"Answer to Question 2: {result_2}")

    result_3=operation_three("U07N2S0124.csv", base_directory)
    print(f"Answer to Question 3: {result_3}")

    file_names = [
        "MNZIRS0108.csv",
        "Y1HZ7B0146.csv",
        "U07N2S0124.csv",
        "CT4OAR0154.csv",
        "Y8S4N80139.csv"
    ]
    result_4=operation_four(file_names,base_directory,"MO_BS_AR")
    print(f"Answer to Question 4: {result_4}")

    result_5=operation_four(file_names,base_directory,"MO_BS_NCI")
    print(f"Answer to Question 5: {result_5}")

    # result_6=operation_six(file_names,base_directory,"MO_BS_Goodwill")
    # print(f"Answer to Question 6: {result_6}")


if __name__ == "__main__":
    main()
