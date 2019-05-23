import sys
import pandas as pd
import math


def main(csv_file):
   packets_per_second = 10
   seconds_per_segment = 30
   rows_per_segment = packets_per_second * seconds_per_segment

   f = pd.read_csv(csv_file)
   print(str(len(f)) + " rows in total")
   print("Leftover rows: " + str(len(f) % rows_per_segment))
   keep_col = list(range(3, 31)) + list(range(38, 66))
   new_f = f[keep_col]
   total_files_split = int(len(new_f) / rows_per_segment)
   print("File will be split into " + str(total_files_split) + " files of " + 
           str(rows_per_segment) + " rows each")

   new_f = trim_csv(new_f, rows_per_segment)

   file_name = csv_file[:-4]
   split_and_output_df(new_f, rows_per_segment, file_name)


def split_and_output_df(df, rows_per_segment, file_name):

    counter = 1

    remaining_df = df
    while len(remaining_df) >= rows_per_segment:
        curr_df = pd.DataFrame()
        rest = pd.DataFrame()
        curr_df = remaining_df[:rows_per_segment]
        rest = remaining_df[rows_per_segment:]
        remaining_df = rest
        output_name = file_name + "_trimmed_" + str(counter) + ".csv"
        print("Outputting " + output_name)
        curr_df.to_csv(output_name, index=False, header=False)
        counter += 1


def trim_csv(df, rows_per_segment):
   leftover_rows = len(df) % rows_per_segment

   print("Removing " + str(math.floor(leftover_rows / 2)) + " rows from beginning")
   df = df.iloc[math.floor(leftover_rows / 2):]

   print("Removing " + str(math.ceil(leftover_rows / 2)) + " rows from end")
   df = df.iloc[:-math.ceil(leftover_rows / 2)]

   print(str(len(df)) + " rows in total")

   return df


if __name__ == "__main__":
   main(sys.argv[1])
