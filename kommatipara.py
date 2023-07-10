from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *
from datetime import date, datetime
import spark

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("MyApp").getOrCreate()

def log(log_date,error_desc, log_text, row_count):
  schema= StructType([StructField("log_date",StringType(),True),
                      StructField("status",StringType(), True),
                      StructField("log_text", StringType(), True),
                      StructField("row_count",IntegerType(), True),
  ])
  success_list =[(log_date,error_desc,log_text,row_count)]
  logdf = spark.createDataFrame(success_list,schema)
  current_date = str(date.today())
  current_timestamp = str(datetime.strftime(datetime.now(),"%Y%m%d%H%M%S"))
  #logdf.show()
  logdf.coalesce(1).write.mode('append').csv('/content/logfile.csv')

#log(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),'Success','Data read test',1)

# run to check log data
logdata = spark.read.csv('/content/logfile.csv')
logdata.show()

# three parameters as per requirement
# parameters can be passed through dbutils

# dbutils.widgets.text('ds1path')
# ds1_path = dbutils.widgets.get('ds1path)

# dbutils.widgets.multiselect('country','Netherlands',['Netherlands','United Kingdom','United States'])
# country_names = [dbutils.widgets.get('country')]

ds1_path='/content/dataset_one.csv'
ds2_path='/content/dataset_two.csv'
country_names = ['United Kingdom', 'Netherlands']

# Read dataset_one and dataset_two based on the paths provided

try:
  ds1 = spark.read.csv(ds1_path, header=True)
  ds1.show()
  # ds1 has country
  ds2 = spark.read.csv(ds2_path, header=True)
  ds2.show()
  # ds2 financial details
  log(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),'Success','Dataset 1 read successful',ds1.count())
  log(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),'Success','Dataset 2 read successful',ds2.count())
except Exception as e:
  log(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),'Failed',e.args,0)

# generic function to filter based on country or list of countries

def to_filter(df, country_name):
  df_filtered = df.filter(df['country'].contains(country_name))
  return df_filtered

# generic function to rename column name to be readable

def rename(df,old_name,new_name):
  df_renamed = df.withColumnRenamed(old_name,new_name)
  return df_renamed

t = ds1.filter(ds1['id']=='0')
t.show()

try:
  for country in country_names:
    print(country)
    f = to_filter(ds1, country)
    f.show()
    t = t.union(f)
  t.show()
  log(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),'Success','Dataset 1 filtered on country successful',t.count())
except Exception as e:
  log(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),'Failed',e.args,0)

# datasets joined on id column - type: inner
# ds is joined dataset with filtered dataset one
try:
  ds = t.join(ds2, how='inner', on='id')
  ds.show()
  log(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),'Success','Join successful',ds.count())
except Exception as e:
  log(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),'Failed',e.args,0)

# renaming columns

try:
  r1=rename(ds,'btc_a','bitcoin_address')
  r2=rename(r1,'cc_t','credit_card_type')
  r3=rename(r2,'cc_n','credit_card_number')
  r3.show()
  log(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),'Success','Rename successful',1)
except Exception as e:
  log(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),'Failed',e.args,0)

# selecting required columns

final_ds = r3.select('email','country','bitcoin_address','credit_card_type')
final_ds.show()