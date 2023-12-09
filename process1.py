import argparse
import os
import pandas as pd
from datetime import datetime, timedelta


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--to_process",
        type=str,
        required=True,
        help="Path to the Parquet file to be processed.",
    )
    parser.add_argument(
        "--output_dir",
        type=str,
        required=True,
        help="The folder to store the resulting CSV files.",
    )
    args = parser.parse_args()

    # Read the Parquet file
    df = pd.read_parquet(args.to_process)
    df.sort_values(by=['unit','timestamp'])

    # Create a new column to track the trip number
    df["trip_number"] = 0

    # Iterate over the rows of the DataFrame
    for row_index, row in df.iterrows():
        # Get the current timestamp
        current_timestamp = datetime.strptime(row["timestamp"], "%Y-%m-%dT%H:%M:%SZ")

        # Get the timestamp of the previous row
        previous_timestamp = None
        if row_index > 0:
            previous_timestamp = datetime.strptime(
                df.loc[row_index - 1]["timestamp"], "%Y-%m-%dT%H:%M:%SZ"
            )

        # If the time difference between the current and previous rows is more than 7 hours, start a new trip
        if previous_timestamp is not None and (current_timestamp - previous_timestamp) > timedelta(hours=7):
            df.loc[row_index:,"trip_number"] = df.loc[row_index,"trip_number"] + 1

    # Group the data by the unit and trip number
    grouped_df = df.groupby(["unit", "trip_number"])

    # Create a CSV file for each group
    for group_name, group in grouped_df:
        #print(group_name[0])
        #print(group)
        if not os.path.exists(args.output_dir):
             os.makedirs(args.output_dir)
             output_file = os.path.join(args.output_dir, f"{group_name[0]}_{group_name[1]}.csv")
             group = group.loc[:,['latitude','longitude','timestamp']]
             group.to_csv(output_file, index=False)
        else:
             output_file = os.path.join(args.output_dir,f"{group_name[0]}_{group_name[1]}.csv")
             group = group.loc[:,['latitude','longitude','timestamp']]
             group.to_csv(output_file, index=False)



if __name__ == "__main__":
    main()

