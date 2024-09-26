# Databricks notebook source
file_path = "abfss://momentum@databricksdap2poc.dfs.core.windows.net"

# COMMAND ----------

folder_names=['de','us']

# COMMAND ----------

def get_dir_content(data_path):
    for dir_path in dbutils.fs.ls(data_path):
        if dir_path.isFile():
            #os.stat gets statistics on a path. st_mtime gets the most recent content modification date time
            yield [dir_path.path,dir_path.modificationTime ]
        elif dir_path.isDir() and data_path != dir_path.path:
            #if the path is a directory, call the function on it again to check its contents
            yield from get_dir_content(dir_path.path)

# COMMAND ----------

for folder in folder_names:
    path = file_path+'/'+folder
    subjects = get_dir_content(path)
    for subject in subjects:
        parts = subject[0].split("/")
        table_name = parts[-1].replace('.parquet','')
        table_name = table_name.replace('.','_')
        table_name= table_name+"_"+parts[3]
        file_name= parts[-1]
        data = spark.read.parquet(path+"/"+file_name)
        data.write.mode("overwrite").saveAsTable("`momentum`.`momentum_test`.`"+table_name+"`")

        print(table_name)

# COMMAND ----------


