# Databricks notebook source
# MAGIC %md
# MAGIC # Driver segmentation
# MAGIC 
# MAGIC This notebook aims to generate the variables for driver segmentation. 
# MAGIC 
# MAGIC **These variables are:**
# MAGIC 
# MAGIC 1. **SLA (Service Level Agreement)**: It's the proportion of orders that the driver delivered ontime.
# MAGIC 2. **Critical delay**: It's the proportion of orders that the driver delivered 5 minutes late (delay > 300 -seconds-).
# MAGIC 3. **Proportion of completed routes**: It's the proportion of routes that the driver completed.
# MAGIC 4. **Days connected**: It's the amount of days that the driver was online.
# MAGIC 5. **Average daily hours online**: It's the hours average daily that the driver was online.
# MAGIC 6. **Logistic region**: It's the logistic region where the driver works.
# MAGIC 
# MAGIC **Window Frame:**
# MAGIC 
# MAGIC The information is extracted with a window frame of 15 days. This ETL runs the first and sixteenth days per month.
# MAGIC 
# MAGIC **Datasources:**
# MAGIC * [logistics_curated.fleet_deliveries_co](https://dataportal.ifoodcorp.com.br/table_detail/master/databricks/logistics_curated/fleet_deliveries_co)
# MAGIC * [logistics_curated.fleet_routes_co](https://dataportal.ifoodcorp.com.br/table_detail/master/databricks/logistics_curated/fleet_routes_co)
# MAGIC * [logistics_curated.fleet_autonomous_supply_co](https://dataportal.ifoodcorp.com.br/table_detail/master/databricks/logistics_curated/fleet_autonomous_supply_co)
# MAGIC 
# MAGIC This information feeds a model that runs with *BruceML*. Here you can find more information about this.
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC For any information about the ETL please contact @sebastian.jimenez

# COMMAND ----------

from ifood_databricks import datalake
from ifood_databricks import etl
from pyspark.sql.functions import col, when
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from datetime import datetime
from datetime import timedelta
from ifood_databricks.toolbelt.data_quality_validator import ifoodDataQualityValidator

# COMMAND ----------

# DBTITLE 1,Dates to extract information
# FINISH_DATE = '2022-08-15'
FINISH_DATE = (datetime.now() - timedelta(days = 1)).strftime('%Y-%m-%d')
START_DATE  = (datetime.strptime(FINISH_DATE, '%Y-%m-%d') - timedelta(days = 14)).strftime('%Y-%m-%d')

# COMMAND ----------

# START_DATE = '2022-09-05'
# FINISH_DATE = '2022-09-19'

# COMMAND ----------

FINISH_DATE

# COMMAND ----------

 print(FINISH_DATE)
 print(START_DATE)

# COMMAND ----------

worker_registration = datalake.read(namespace='logistics', zone='curated',   dataset='fleet_worker_registration_co')
fleet               = datalake.read(zone='curated', namespace='logistics', dataset='fleet_deliveries_co_v2')
workers             = datalake.read(zone = 'raw',     namespace = 'fleet_co',  dataset = 'worker').selectExpr('id as driver_id', 'uuid as driver_uuid')

# COMMAND ----------

worker_registration = worker_registration.join(workers, workers.driver_id==worker_registration.worker_id, 'left')

# COMMAND ----------

display(worker_registration.orderBy(worker_registration.registration_timestamp.desc()))

# COMMAND ----------

# display(worker_registration.where(F.col('worker_state')=='ACTIVE'))

# COMMAND ----------

orders = fleet.groupBy('driver_uuid').agg(F.countDistinct('order_uuid').alias('orders'))

antiguedad = worker_registration.where(F.col('worker_state')=='ACTIVE')\
                                .withColumn('end_date', F.lit(FINISH_DATE))\
                                .withColumn('reg_date', F.to_date('registration_approval_timestamp', "yyyy-MM-dd").cast(F.StringType()))\
                                .withColumn('antiquity', F.datediff(F.col('end_date'), F.col('reg_date')))\
                                .where(F.col('antiquity')<=30)


new_drivers = antiguedad.join(orders, 'driver_uuid', 'left')\
                        .select('driver_uuid', 'logistic_region', 'orders', 'antiquity')\
                        .na.fill(value=0, subset=['orders'])\
                        .where(F.col('orders')<=5)


new_drivers.display()


# COMMAND ----------

# DBTITLE 1,Extract information about Critical Delay per driver
cols_deliveries_list = [
  'driver_id',
  'driver_uuid',
  'order_delivery_uuid',
  'order_number',
  'logistic_region',
  'date_partition',
  'delay',
  'driver_consumer_evaluation',
  'real_delivery_time',
  'expected_delivery_time',
  'time_to_origin',
  'assign_timestamp',
  'pickup_timestamp',
  'at_origin_timestamp',
  'collect_timestamp',
  'expected_timestamp',
  'at_destination_timestamp',
  'real_total_time_to_origin',
  'expected_total_time_to_origin',
  'late_driver_flag',
  'time_to_destination',
  'distance_route'
]

deliveries_df = fleet \
                .where((col('date_partition') >= START_DATE) & (col('date_partition') <= FINISH_DATE)) \
                .where(col('last_status') != 'CANCELLED') \
                .where(col('driver_uuid').isNotNull()) \
                .select(*cols_deliveries_list) \
                .withColumn('is_critical_delay', when( col('delay') > 300, 1 ).otherwise( 0 ) ) \
                .withColumn('is_ontime', when( col('real_delivery_time') <= col('expected_delivery_time'), 1 ).otherwise( 0 ) ) \
                .withColumn('driver_delay_origin', when (col('late_driver_flag')=='true',1).otherwise(0))\
                .withColumn('time to destination (mins)',(col('time_to_destination')/3600))\
                .withColumn('speed', col('distance_route')/(col('time_to_destination')/3600))\
                .withColumn('logistic_region', F.initcap('logistic_region'))


# rank by driver_id the logistic_region regarding the number orders delivered
driver_region_df = deliveries_df \
                   .groupBy('driver_uuid', 'logistic_region') \
                   .agg(F.count('order_delivery_uuid').alias('count_orders'))

driver_region_df = driver_region_df \
                   .withColumn("rank", 
                               F.dense_rank().over( Window.partitionBy("driver_uuid").orderBy(F.desc("count_orders")) )
                               ) \
                   .where(col('rank') == 1) \
                   .select(*['driver_uuid', 'logistic_region'])

# # ==================================================================
driver_kpis_df = deliveries_df \
                  .groupBy('driver_uuid') \
                  .agg(
                       F.round(F.mean('is_critical_delay'), 4).alias('prop_critical_delay'),
                       F.round(F.mean('driver_delay_origin'), 4).alias('driver_delay_origin'),
                       F.round(F.mean('speed'), 4).alias('speed'),
                       F.round(F.mean('driver_consumer_evaluation'),4).alias('consumer_evaluation') 
                      )

driver_df = driver_region_df.join(driver_kpis_df, 
                                 on = ['driver_uuid'],
                                 how = 'left')

# COMMAND ----------

driver_df.count()

# COMMAND ----------

# DBTITLE 1,Extract information about the proportion of completed routes
curated_routes = datalake.read(namespace = 'logistics', 
                               zone      = 'curated',
                               dataset   = 'fleet_routes_co') \


curated_routes = curated_routes.join(drivers, curated_routes.worker_id==drivers.driver_id, 'left')\
                                .where((col('date_partition') >= START_DATE) & (col('date_partition') <= FINISH_DATE)) \
                                .withColumn('route_status', when( col("cancel_rejection_type").isNull(), 'completed')
                                                            .otherwise(col('cancel_rejection_type'))) \
                                .groupBy('driver_uuid') \
                                .agg(F.sum(when( col("route_status") == 'completed', 1).otherwise(0)).alias('completed_routes'),
                                      F.count('route_status').alias("total_routes")) \
                                .withColumn( 'prop_completed_routes', col('completed_routes') / col('total_routes') ) \
                                .select('driver_uuid', 'prop_completed_routes')

# COMMAND ----------

# DBTITLE 1,Extract information about connection
cols_supply_list = [
  'driver_uuid',
  'date_partition',
  'interval_start_timestamp',
  'interval_executed_seconds'
]

autonomous_orig_df = datalake.read(zone      = 'curated',
                                   namespace = 'logistics',
                                   dataset   = 'fleet_autonomous_supply_co') \
                    .select(*cols_supply_list) \
                    .where((col('date_partition') >= START_DATE) & (col('date_partition') <= FINISH_DATE)) \
                    .withColumn('day', F.dayofweek('interval_start_timestamp'))\
                    .withColumn('hour', F.hour('interval_start_timestamp'))\
                    .withColumnRenamed('worker_id', 'driver_id') \
                    .withColumn('interval_total_executed_hours', (col('interval_executed_seconds') / 3600))\
                    .withColumn('interval_executed_critical_hours', when(((F.hour('interval_start_timestamp')>=11) & (F.hour('interval_start_timestamp')<14) |                                                                    (F.hour('interval_start_timestamp')>=18) & (F.hour('interval_start_timestamp')<20)) & (col('interval_executed_seconds')>0),col('interval_executed_seconds') / 3600).otherwise(0))\
                    .withColumn('interval_executed_special_hours', when((((F.col('day')==6)&(F.col('hour')>=18))|F.col('day').isin(7,1)),col('interval_executed_seconds') / 3600).otherwise(0))\
                    
                    

autonomous_df = autonomous_orig_df \
                .groupBy('driver_uuid', 'date_partition') \
                .agg(F.sum('interval_executed_critical_hours').alias('critical_hours_executed'), F.sum('interval_executed_special_hours').alias('hours_special'), F.sum('interval_total_executed_hours').alias('hours_executed'))\
                .groupBy('driver_uuid') \
                .agg(F.count('date_partition').alias('days_connected'),
                     F.mean('critical_hours_executed').alias('avg_daily_critical_hours_executed'),
                     F.mean('hours_special').alias('avg_special_hours_executed'),
                     F.mean('hours_executed').alias('avg_total_hours_executed'))


autonomous_df.show()




#  .where(((F.hour('interval_start_timestamp')>=11) & (F.hour('interval_start_timestamp')<=14) |                                                                    (F.hour('interval_start_timestamp')>=18) & (F.hour('interval_start_timestamp')<=20)) & (col('interval_executed_seconds')>0)
#                           )\

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Extract information about UTR
autonomous_orig_df2 = datalake.read(zone      = 'curated',
                                   namespace = 'logistics',
                                   dataset   = 'fleet_autonomous_supply_co') \
                    .select(*cols_supply_list) \
                    .where((col('date_partition') >= START_DATE) & (col('date_partition') <= FINISH_DATE)) \
                    .where(col('interval_executed_seconds')>0)\
                    .withColumn('interval_executed_hours', col('interval_executed_seconds') / 3600)

order_per_day = deliveries_df \
                .groupby('driver_uuid','date_partition')\
                .agg(F.countDistinct(col('order_delivery_uuid')).alias('ordenes'))

union = (autonomous_orig_df2.driver_uuid == order_per_day.driver_uuid) & (autonomous_orig_df2.date_partition == order_per_day.date_partition)

utr_per_day = autonomous_orig_df2.alias('a') \
                .groupBy('driver_uuid', 'date_partition') \
                .agg(F.sum('interval_executed_hours').alias('hours_executed')) \
                .join(order_per_day.alias('o'),on=union,how='left')\
                .withColumn('utr_per_day', col('ordenes')/col('hours_executed'))

utr_df = utr_per_day.groupBy('a.driver_uuid')\
                    .agg(F.mean('utr_per_day').alias('avg_daily_utr'))
                 

# COMMAND ----------

# display(utr_per_day)

# COMMAND ----------

# DBTITLE 1,Extract information about Strikes
strikes = datalake.read(zone = 'curated',
                              namespace = 'latam',
                              dataset = 'datamart_co_driver_strikes')\

strikes = strikes.join(workers, 'driver_id', 'left')


strikes = strikes.where((col('created_date') <= FINISH_DATE))\
                  .where(~(col('strike_classification').isin('DEVICE_LOSS','LONG_PERIOD_OFFLINE','NEW_DRIVER','FIRST_ORDER','DAYS_IN_NEGATIVE')))\
                  .groupby(col('driver_uuid'))\
                  .agg(F.countDistinct(col('strike_id')).alias('strikes'))

# COMMAND ----------

# DBTITLE 1,Extract information about Wallet recharge
wallet_recharge = datalake.read(zone = 'curated',
                              namespace = 'logistics',
                              dataset = 'fleet_billing_co')\
                          .where((col('date_partition') >= START_DATE) & (col('date_partition') <= FINISH_DATE))\
                          .where((col('billing_cause').isin('TOP_UP','TOP_UP_MANUAL','WALLET_RECHARGE')))\
                          .groupby(col('driver_uuid'))\
                          .agg(F.countDistinct(col('billing_id')).alias('qty recharge'),
                               F.round(F.avg(col('quantity')),4).alias('avg recharge')
                              )

# COMMAND ----------

# DBTITLE 1,Extract information about Wallet balance
wallet_balance = datalake.read(zone = 'curated', namespace = 'latam', dataset = 'drivers_wallet_streak_co')\
                         .withColumnRenamed('worker_id', 'driver_id')
                        

wallet_balance = wallet_balance.join(workers, 'driver_id', 'left')
                         
wallet_balance = wallet_balance.where((col('date_partition') >= START_DATE) & (col('date_partition') <= FINISH_DATE))\
                               .groupby(col('driver_uuid'))\
                               .agg((F.countDistinct(when(col('sign')>=0, col('date_partition')))/(F.datediff(F.max(col('date_partition')),F.min(col('date_partition'))) + 1)).alias('% no negative wallet')
                              )

# COMMAND ----------

# DBTITLE 1,Join tables
driver_join_df = driver_df.join(curated_routes , on = 'driver_uuid', how = 'left')
driver_join_df = driver_join_df.join(autonomous_df, on = 'driver_uuid', how = 'left')
driver_join_df = driver_join_df.join(strikes, on = 'driver_uuid', how = 'left')
driver_join_df = driver_join_df.join(wallet_recharge, on = 'driver_uuid', how = 'left')
driver_join_df = driver_join_df.join(wallet_balance, on = 'driver_uuid', how = 'left')
driver_join_df = driver_join_df.join(utr_df, on = 'driver_uuid', how = 'left')

# COMMAND ----------

driver_join_df.count()

# COMMAND ----------

# DBTITLE 1,Fill Null values
driver_join_df = driver_join_df.na.fill(value=0,subset=["strikes","qty recharge","avg recharge","avg_daily_utr","% no negative wallet"])

# COMMAND ----------

def fill_with_mean(df, columns): 
    stats = df.agg(*(
        F.avg(c).alias(c) for c in df.columns if c in columns
    ))
    return df.na.fill(stats.first().asDict())

driver_join_df=fill_with_mean(driver_join_df, ["speed",'consumer_evaluation',"days_connected","avg_daily_critical_hours_executed"])

# COMMAND ----------

#Checking if there are null values
driver_join_df.select([F.count(when(col(c).isNull(), c)).alias(c) for c in driver_join_df.columns]).display()

# COMMAND ----------

# DBTITLE 1,Date column
driver_join_df = driver_join_df.withColumn('date_partition', F.lit(FINISH_DATE))

# COMMAND ----------

driver_join_df = driver_join_df.dropna()

# COMMAND ----------

# driver_join_df.display()

# COMMAND ----------

driver_join_df.count()

# COMMAND ----------

final = driver_join_df.join(new_drivers, (['driver_uuid', 'logistic_region']), 'leftouter')

# COMMAND ----------

final.display()

# COMMAND ----------

test = final.where(col('logistic_region').isin('Bogota', 'Cali', 'Barranquilla', 'Medellin', 'Palmira', 'Soledad', 'Monteria', 'Valledupar', 'Santa Marta', 'Manizales'))
test.display()

# COMMAND ----------

test.count()

# COMMAND ----------

5517-5174

# COMMAND ----------

test \
    .groupby(['driver_uuid']) \
    .count() \
    .where('count > 1') \
    .sort('count', ascending=False) \
    .show()

# COMMAND ----------

# DBTITLE 1,Save data
# etl.dataframe2curated(driver_join_df,
#                       namespace    = "logistics",
#                       dataset      = "co_features_new_driver_segmentation",
#                       partition_by = ["date_partition"],
#                       as_delta     = True,
# #                      delta_mode   = "batch-s3-full"
#                       delta_mode   = "batch-s3-append",
#                       is_batch_append   = True,
#                       dynamic_partition = True
#                       )

# COMMAND ----------

# DBTITLE 1,Validation data
# df = datalake.read(zone = "curated", 
#                    namespace = "logistics",
#                    dataset = "co_features_new_driver_segmentation")

# COMMAND ----------

# def quality_checker(df, display_name):
  
#   validator = ifoodDataQualityValidator(df, 
#                                         displayName = display_name, 
#                                         slackUsername = "@sebastian.jimenez", 
#                                         level = "CRITICAL")

#   check = validator \
#           .isNeverNull("driver_id") \
#           .isNeverNull("logistic_region") \
#           .isNeverNull("prop_critical_delay") \
#           .isNeverNull("sla") \
#           .isNeverNull("prop_completed_routes") \
#           .isNeverNull("days_connected") \
#           .isNeverNull("avg_daily_hours_executed") \
#           .hasNumRowsGreaterThan(1000)
  
#   validator.run(check)

# COMMAND ----------

# quality_checker(df, display_name = "co_features_new_driver_segmentation")

# COMMAND ----------

# from ifood_databricks.utils.dataframeutils import DataframeUtils
# DataframeUtils.download_dataframe(df)

# COMMAND ----------

# %sql
# select * 
# from latam_sandbox.co_driver_segmentation_nb_new
# --where driver_id = 136579

# COMMAND ----------


