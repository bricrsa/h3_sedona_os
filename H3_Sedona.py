# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import *
import h3_pyspark
from sedona.register import SedonaRegistrator  
from sedona.utils import SedonaKryoRegistrator, KryoSerializer
SedonaRegistrator.registerAll(spark)

# COMMAND ----------

# MAGIC %fs rm -r dbfs:/mnt/land-registry/transformed/osdatahub/h3_lt200_overlap/

# COMMAND ----------

# MAGIC %fs rm -r dbfs:/mnt/land-registry/transformed/osdatahub/h3_ge200_overlap/

# COMMAND ----------

# MAGIC %fs rm -r dbfs:/mnt/land-registry/transformed/osdatahub/h3_ge250_overlap/

# COMMAND ----------

# %fs ls dbfs:/mnt/land-registry/raw/osdatahub/
# %fs ls dbfs:/mnt/land-registry/transformed/osdatahub/

# /mnt/land-registry/transformed/osdatahub/h3_lt200_overalap/
# /mnt/land-registry/transformed/osdatahub/h3_ge200_overalap/

# COMMAND ----------

read_path = 'dbfs:/mnt/land-registry/raw/osdatahub/osopenuprn_202201_csv'

# load os open data 
# https://www.ordnancesurvey.co.uk/business-government/products/open-uprn
# Contains OS data © Crown copyright and database right 2021
# https://www.ordnancesurvey.co.uk/business-government/tools-support/os-net/coordinates
os_open_schema = StructType([
  StructField("UPRN", LongType(), True)
  ,StructField("X_COORDINATE", DecimalType(), True) #EASTING
  ,StructField("Y_COORDINATE", DecimalType(), True) #NORTHING
  ,StructField("LATITUDE", DoubleType(), True)
  ,StructField("LONGITUDE", DoubleType(), True)
])


# load file and add H3 
osdf = spark.read.csv(read_path,header=True, schema=os_open_schema)
osdf = osdf.withColumn('h3_08', h3_pyspark.geo_to_h3('LATITUDE', 'LONGITUDE', F.lit(8))) \

osdf.createOrReplaceTempView('osopenuprn_202201_h3' )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from osopenuprn_202201_h3 limit 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC create temp view osopenuprn_202201_lt200_h3
# MAGIC as
# MAGIC select h3_08 
# MAGIC from osopenuprn_202201_h3
# MAGIC group by h3_08
# MAGIC having count(UPRN) < 200

# COMMAND ----------

# MAGIC %sql 
# MAGIC --overlap 
# MAGIC 
# MAGIC drop table if exists osopenuprn_202201_overlap;
# MAGIC 
# MAGIC create table osopenuprn_202201_overlap
# MAGIC location '/mnt/land-registry/transformed/osdatahub/h3_lt200_overlap/'
# MAGIC as
# MAGIC select 
# MAGIC   i.UPRN
# MAGIC   , i.X_COORDINATE
# MAGIC   , i.Y_COORDINATE
# MAGIC   , o.UPRN as oUPRN
# MAGIC   , o.X_COORDINATE as oX_COORDINATE
# MAGIC   , o.Y_COORDINATE as oY_COORDINATE
# MAGIC   , ST_DISTANCE(ST_Point(decimal(i.X_COORDINATE), decimal(i.Y_COORDINATE)),ST_Point(decimal(o.X_COORDINATE), decimal(o.Y_COORDINATE))) as stdist
# MAGIC from 
# MAGIC   osopenuprn_202201_h3 i
# MAGIC   inner join osopenuprn_202201_lt200_h3 s on s.h3_08 = i.h3_08
# MAGIC   inner join osopenuprn_202201_h3 o on i.h3_08 = o.h3_08
# MAGIC where 
# MAGIC   i.UPRN != o.UPRN

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from osopenuprn_202201_overlap

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select RecCount, count (UPRN) as UPRNCount
# MAGIC from
# MAGIC   (select UPRN, count(*) as RecCount from osopenuprn_202201_overlap 
# MAGIC   group by UPRN) d
# MAGIC group by RecCount
# MAGIC order by UPRNCount desc
# MAGIC limit 2000

# COMMAND ----------

# MAGIC %sql
# MAGIC drop view if exists osopenuprn_202201_ge200_h3;
# MAGIC 
# MAGIC create temp view osopenuprn_202201_ge200_h3
# MAGIC as
# MAGIC select h3_08 
# MAGIC from osopenuprn_202201_h3
# MAGIC group by h3_08
# MAGIC having count(UPRN) >= 200 and count(UPRN) < 250

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select RecCount, count (h3_08) as UPRNCount
# MAGIC from
# MAGIC   (select s.h3_08, count(*) as RecCount 
# MAGIC   from osopenuprn_202201_h3 s
# MAGIC     inner join osopenuprn_202201_ge200_h3 j on s.h3_08 = j.h3_08
# MAGIC   group by s.h3_08) d
# MAGIC group by RecCount
# MAGIC order by UPRNCount desc
# MAGIC limit 2000

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC drop table if exists osopenuprn_202201_ge200_overlap;
# MAGIC 
# MAGIC create table osopenuprn_202201_ge200_overlap
# MAGIC location '/mnt/land-registry/transformed/osdatahub/h3_ge200_overlap/'
# MAGIC as
# MAGIC select * 
# MAGIC from 
# MAGIC   (
# MAGIC   select 
# MAGIC     *
# MAGIC     , dense_rank() OVER (PARTITION BY UPRN ORDER BY stdist DESC) as dist_rank 
# MAGIC   from
# MAGIC     (
# MAGIC     select 
# MAGIC       i.UPRN
# MAGIC       , i.X_COORDINATE
# MAGIC       , i.Y_COORDINATE
# MAGIC       , o.UPRN as oUPRN
# MAGIC       , o.X_COORDINATE as oX_COORDINATE
# MAGIC       , o.Y_COORDINATE as oY_COORDINATE
# MAGIC       , ST_DISTANCE(ST_Point(decimal(i.X_COORDINATE), decimal(i.Y_COORDINATE)),ST_Point(decimal(o.X_COORDINATE), decimal(o.Y_COORDINATE))) as stdist
# MAGIC     from 
# MAGIC       osopenuprn_202201_h3 i
# MAGIC       inner join osopenuprn_202201_ge200_h3 s on s.h3_08 = i.h3_08
# MAGIC       inner join osopenuprn_202201_h3 o on i.h3_08 = o.h3_08
# MAGIC     where 
# MAGIC       i.UPRN != o.UPRN
# MAGIC     ) r
# MAGIC   )
# MAGIC   where dist_rank  <=200

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from osopenuprn_202201_ge200_overlap

# COMMAND ----------

# MAGIC %sql
# MAGIC drop view if exists osopenuprn_202201_ge250_h3;
# MAGIC 
# MAGIC create temp view osopenuprn_202201_ge250_h3
# MAGIC as
# MAGIC select h3_08 
# MAGIC from osopenuprn_202201_h3
# MAGIC group by h3_08
# MAGIC having count(UPRN) >= 250

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --understand how many UPRNs have not been assigned yet and how many H3_08 that relates to
# MAGIC 
# MAGIC select RecCount, count (h3_08) as UPRNCount
# MAGIC from
# MAGIC   (select s.h3_08, count(*) as RecCount 
# MAGIC   from osopenuprn_202201_h3 s
# MAGIC     inner join osopenuprn_202201_ge250_h3 j on s.h3_08 = j.h3_08
# MAGIC   group by s.h3_08) d
# MAGIC group by RecCount
# MAGIC order by UPRNCount desc
# MAGIC limit 2000

# COMMAND ----------

# MAGIC %sql
# MAGIC drop view if exists osopenuprn_202201_ge250_h3_short;
# MAGIC 
# MAGIC create temp view osopenuprn_202201_ge250_h3_short
# MAGIC as
# MAGIC select UPRN
# MAGIC   , X_COORDINATE
# MAGIC   , Y_COORDINATE
# MAGIC   , s.h3_08 
# MAGIC from osopenuprn_202201_h3 s
# MAGIC   inner join osopenuprn_202201_ge250_h3 j on s.h3_08 = j.h3_08

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC drop table if exists osopenuprn_202201_ge250_overlap;
# MAGIC 
# MAGIC create table osopenuprn_202201_ge250_overlap
# MAGIC location '/mnt/land-registry/transformed/osdatahub/h3_ge250_overlap/'
# MAGIC as
# MAGIC select * 
# MAGIC from 
# MAGIC   (
# MAGIC   select 
# MAGIC     *
# MAGIC     , dense_rank() OVER (PARTITION BY UPRN ORDER BY stdist asc) as dist_rank 
# MAGIC   from
# MAGIC     (
# MAGIC     select 
# MAGIC       i.UPRN
# MAGIC       , i.X_COORDINATE
# MAGIC       , i.Y_COORDINATE
# MAGIC       , o.UPRN as oUPRN
# MAGIC       , o.X_COORDINATE as oX_COORDINATE
# MAGIC       , o.Y_COORDINATE as oY_COORDINATE
# MAGIC       , ST_DISTANCE(ST_Point(decimal(i.X_COORDINATE), decimal(i.Y_COORDINATE)),ST_Point(decimal(o.X_COORDINATE), decimal(o.Y_COORDINATE))) as stdist
# MAGIC     from 
# MAGIC       osopenuprn_202201_ge250_h3_short i
# MAGIC       inner join osopenuprn_202201_ge250_h3_short o on i.h3_08 = o.h3_08
# MAGIC     where 
# MAGIC       i.UPRN != o.UPRN
# MAGIC       and ST_DISTANCE(ST_Point(decimal(i.X_COORDINATE), decimal(i.Y_COORDINATE)),ST_Point(decimal(o.X_COORDINATE), decimal(o.Y_COORDINATE))) < 2000
# MAGIC     ) r
# MAGIC   )
# MAGIC   where dist_rank  <=200

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from osopenuprn_202201_ge250_overlap

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from osopenuprn_202201_ge250_overlap 
# MAGIC where UPRN = 90055535 
# MAGIC order by dist_rank desc
# MAGIC limit 1000

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select RecCount, count (UPRN) as UPRNCount
# MAGIC from
# MAGIC   (select UPRN, count(*) as RecCount from osopenuprn_202201_ge250_overlap 
# MAGIC   group by UPRN) d
# MAGIC group by RecCount
# MAGIC order by UPRNCount desc
# MAGIC limit 2000
