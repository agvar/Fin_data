
import pandas as pd
import numpy as np
import os
import datetime

#read csv into  a dataframe
def split_input_files_by_quater(input_dir_name,input_file_prefix,output_dir_name,file_extension)  :

    total_group_count=0

    input_file_path=os.path.join(input_dir_name, input_file_prefix + "." + file_extension)
    #df=pd.read_csv("C:\\Users\\User\\Downloads\\accepted_2007_to_2018Q4.csv")
    input_df=pd.read_csv(input_file_path)

    #add a new column for Quater and Year
    input_df["Quater"]=pd.PeriodIndex(pd.to_datetime(input_df.issue_d), freq='Q').astype(str)

    #create groups by quater and write to separate output files
    grouped=input_df.groupby(["Quater"])
    for group,group_df in grouped:
        output_file_prefix='P2Pfin_data_'+group
        output_file=os.path.join(output_dir_name,output_file_prefix + "." + file_extension)
        group_df.to_csv(output_file)
        total_group_count=total_group_count+len(group_df.index)

    # check if the total file count matches the sum of group counts
    input_count=len(input_df.index)
    if input_count==total_group_count:
        print("Counts match- files split by Quater")
        return 1
    else:
        return -1


if __name__=='__main__':
    input_dir_name = 'C:\\Users\\User\\Downloads'
    input_file_prefix = 'accepted_2007_to_2018Q4'
    output_dir_name = 'C:\\Users\\User\\Downloads\\P2P_Files_by_Quater'
    file_extension = 'csv'

    start_time=datetime.datetime.now()
    split_status=split_input_files_by_quater(input_dir_name, input_file_prefix, output_dir_name, file_extension)
    end_time = datetime.datetime.now()

    if split_status==1:
        print(f"Split process completed in {end_time-start_time}")
    else:
        print(f"Split process Failed")






