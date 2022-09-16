# Sorts a tsv by all columns except the first, because hadoop apparently has a lot of trouble with that???
# 09/16/22 itisluiz
import os
import pandas as pd

dir_jobs = os.getcwd() + "/../data/out/"
available_jobs = [file.name for file in os.scandir(dir_jobs) if file.is_dir()]

print("Available jobs:")
for index_job in range(len(available_jobs)):
    print(f"{index_job}: {available_jobs[index_job]}")

index_selectedjob = None
while (index_selectedjob is None or (index_selectedjob < 0 or index_selectedjob > len(available_jobs) - 1)):
    try:
        index_selectedjob = int(input("Job index: "))
    except ValueError:
        pass

dir_selectedjob = dir_jobs + available_jobs[index_selectedjob]

df_jobresult = pd.read_csv(dir_selectedjob + "/part-r-00000", sep="\t", header=None)
print(f"Read {len(df_jobresult)} lines")

df_jobresultsorted = df_jobresult.sort_values(by=list(range(1, len(df_jobresult.columns))), ascending=False)
df_jobresultsorted.to_csv(dir_selectedjob + "/sorted_job.csv", sep="\t", index=False, header=None)
print("Written to \"sorted_job.csv\"")