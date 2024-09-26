# Databricks notebook source
#file_path = "https://test@dappocdevstorage.blob.core.windows.net"
file_path = "abfss://momentum@databricksdap2poc.dfs.core.windows.net"

# COMMAND ----------

folder_names=['de','us']
print(folder_names)


# COMMAND ----------

import os  ,shutil 

# COMMAND ----------

entries = os.listdir('my_directory/')

# COMMAND ----------



# COMMAND ----------

import os,shutil
root="/home"


destination="/tmp"
directory = os.path.join(root,"mydir")
os.chdir(directory)
for file in os.listdir("."):
    flag=""
    #check contents of file ?
    for line in open(file):
       if "something" in line:
           flag="found"
    if flag=="found":
       try:
           # or use os.rename() on local
           shutil.move(file,destination)
       except Exception,e: print e
       else:
           print "success"

# COMMAND ----------

#read in the data to dataframe df
df = spark.read.format("csv").option("inferSchema", "true").option("header", "true").option("delimiter";";").load(file_path)
#display the dataframe
display(df)

# COMMAND ----------

print(file_locations)

# COMMAND ----------

#library:copy the data between folders
import shutil

# COMMAND ----------

#to connect file system utility allows you to access Databricks File System (DBFS)
def get_dir_content(data_path):
    # directory/folder path
    for dir_path in dbutils.fs.ls(data_path):
        if dir_path.isFile():
            #os.stat gets statistics on a path. st_mtime gets the most recent content modification date time
            yield [dir_path.path,dir_path.modificationTime ]
        elif dir_path.isDir() and data_path != dir_path.path:
            #if the path is a directory, call the function on it again to check its contents
            yield from get_dir_content(dir_path.path)
            print(path)

# COMMAND ----------

print (get_dir_content)

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
        print(data)
        #data.write.mode("overwrite").saveAsTable("`momentum`.`default`.`"+table_name+"`")

# COMMAND ----------

display(table_name)

# COMMAND ----------

display(data)
