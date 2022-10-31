# Databricks notebook source
from ifood_databricks import datalake, etl
from ifood_databricks.utils.stringutils import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime, date, timedelta
import pyspark.sql.functions as f
from pyspark.sql import Window as W

# COMMAND ----------

orders = datalake.read(namespace='logistics', zone='curated', dataset='fleet_wallet_per_hub_co')
segmentation  = datalake.read(namespace='logistics', zone='curated', dataset='co_ml_driver_segmentation_full')
billing_data  = datalake.read(namespace='fleet_co',  zone='raw', dataset='public.billing_data').withColumnRenamed('worker_id',  'driver_id')
daily_wallet  = datalake.read(namespace='logistics', zone='curated', dataset='fleet_wallet_co').withColumnRenamed('worker_id',  'driver_id')
strikes = datalake.read(zone = 'curated', namespace = 'latam', dataset = 'datamart_co_driver_strikes')
antiguedad = datalake.read(zone = 'curated', namespace = 'logistics', dataset = 'fleet_deliveries_co_V2')
autonomous = datalake.read(namespace='logistics',zone='curated', dataset='fleet_autonomous_supply_co')
curated_routes = datalake.read(namespace = 'logistics',  zone = 'curated', dataset = 'fleet_routes_co')

# COMMAND ----------

from ifood_databricks.data_products.reader import read

workers = read(data_product='logistics_driver', dataset='account_updated', options={'stage': 'gold', 'type': 'batch'}).where('tenant = "co"')

orders       = orders.join(workers['driver_id', 'driver_uuid'], 'driver_id', 'left')
segmentation = segmentation.join(workers['driver_id', 'driver_uuid'], 'driver_id', 'left')
billing_data = billing_data.join(workers['driver_id', 'driver_uuid'], 'driver_id', 'left')
orders = orders.join(workers['driver_id', 'driver_uuid'], 'driver_id', 'left')
orders = orders.join(workers['driver_id', 'driver_uuid'], 'driver_id', 'left')
orders = orders.join(workers['driver_id', 'driver_uuid'], 'driver_id', 'left')
orders = orders.join(workers['driver_id', 'driver_uuid'], 'driver_id', 'left')
orders = orders.join(workers['driver_id', 'driver_uuid'], 'driver_id', 'left')
orders = orders.join(workers['driver_id', 'driver_uuid'], 'driver_id', 'left')

# COMMAND ----------

# segmentation = segmentation.where(col('segment')!='Churn')\
#                            .where(col('segment')!='Inactive')

# COMMAND ----------

display(strikes.where(col('strike_classification')=='NEW_DRIVER'))

# COMMAND ----------

display(segmentation.orderBy(col('segmentation_date').desc()))

# COMMAND ----------

PERIOD = 30
#END_DATE = '2021-10-03'
#START_DATE = '2021-09-04'
END_DATE = str((datetime.today() - timedelta(days=1)).date())
START_DATE =  str((datetime.today() - timedelta(days=PERIOD)).date())
START_DATE, END_DATE 

# COMMAND ----------

df_recharges = billing_data\
      .where("day >= date('{}') and day <= date('{}')".format(START_DATE, END_DATE))\
      .filter(col('billing_cause').isin('TOP_UP','WALLET_RECHARGE','TOP_UP_MANUAL'))\
      .withColumnRenamed('worker_id','driver_id')\
      .withColumn('quantity',col('quantity')/100)\
      .groupBy('driver_id')\
      .agg(count('id').alias('recharges'),
           mean('quantity').alias('avg_recharge'),
           countDistinct('day').alias('days_with_recharges'),
           sum('quantity').alias('total_recharges'))

df_recharges.display()

# COMMAND ----------

df_orders = orders\
  .where("date_partition >= date('{}') and date_partition <= date('{}')".format(START_DATE, END_DATE))\
  .withColumn('negative_orders',when(col('wallet_before_order')<0,lit(1)).otherwise(lit(0)))\
  .withColumn('cash_orders',when(col('payment_type')=='MONEY',lit(1)).otherwise(lit(0)))\
  .orderBy('date_timestamp')\
  .groupBy('driver_id')\
  .agg(mean('negative_orders').alias('perc_negative_orders'),
       mean('cash_orders').alias('perc_cash_orders'),
       count('order_number').alias('orders'),
       first('logistic_region').alias('logistic_region'))

# df_orders.display()

# COMMAND ----------

df_wallet = daily_wallet\
              .withColumn('date_data',date_add(col('date_data'),-1))\
              .where("date_data >= date('{}') and date_data <= date('{}')".format(START_DATE, END_DATE))\
              .selectExpr('worker_id as driver_id',
                          'date_data as date',
                          'wallet')\
              .withColumn('negative_wallet',when(col('wallet')<0,lit(1)).otherwise(lit(0)))\
              .groupBy('driver_id')\
              .agg(sum('negative_wallet').alias('days_negative'),
                   mean('wallet').alias('avg_wallet'),
                   min('wallet').alias('min_wallet'))

# COMMAND ----------

df_strikes = datalake.read(zone = 'curated',
                              namespace = 'latam',
                              dataset = 'datamart_co_driver_strikes')\
                .where(("created_date >= date('{}') and created_date <= date('{}')".format(START_DATE, END_DATE)))\
                .where(~(col('strike_classification').isin('DEVICE_LOSS','LONG_PERIOD_OFFLINE', 'FIRST_ORDER', 'NEW_DRIVER', 'MANUAL_BLOCK_CO', 'DAYS_IN_NEGATIVE', 'REASSIGNED_ORDERS')))\
                .groupby(col('driver_id'))\
                .agg(f.countDistinct(col('strike_id')).alias('strikes'))

# df_stikes = df_strikes.na.fill(value=0, subset='strikes')

# COMMAND ----------

from pyspark.sql.functions import current_date


df_antiguedad = antiguedad\
                    .where(col('last_status')=='COMPLETED')\
                    .groupBy('driver_id')\
                    .agg(min('date_partition').alias('first_order'))\
                    .withColumn("today", current_date().cast("string"))\
                    .withColumn('dias_antiguedad', datediff(col('today'),col('first_order')))\
                    .select('driver_id', 'first_order', 'today', 'dias_antiguedad')

# df_antiguedad.display()

# COMMAND ----------

df_online = autonomous\
            .where((col('date_partition') >= START_DATE) & (col('date_partition') <= END_DATE)) \
                           .groupby(col('driver_uuid'))\
                           .agg(sum((col('interval_executed_seconds')/3600)).alias('online_hours'),
                               sum((col('interval_worked_seconds')/3600)).alias('working_hours')
                               )\

# df_online.display()

# COMMAND ----------

df_rechazos = curated_routes\
                .where((col('date_partition') >= START_DATE) & (col('date_partition') <= END_DATE)) \
                .withColumn('route_status', when( col("cancel_rejection_type").isNull(), 'completed')
                                            .otherwise(col('cancel_rejection_type'))) \
                .groupBy('driver_id') \
                .agg(f.sum(when( col("route_status") == 'completed', 1).otherwise(0)).alias('completed_routes'),
                      f.count('route_status').alias("total_routes")) \
                .withColumn( 'prop_completed_routes', col('completed_routes') / col('total_routes') ) \
                .withColumn('prop_declined_orders', 1-col('prop_completed_routes'))\
                .select('driver_id', 'prop_completed_routes', 'prop_declined_orders')

df_rechazos.display()

# COMMAND ----------

df_workers = df_orders\
    .join(segmentation\
            .selectExpr('driver_id',
                        'segment as segmentation_def'),['driver_id'],'left')\
    .join(df_wallet,['driver_id'],'left')\
    .join(df_strikes, ['driver_id'], 'left')\
    .join(df_antiguedad, ['driver_id'], 'left')\
    .join(df_recharges,['driver_id'],'left')\
    .join(df_online, ['driver_id'], 'left')\
    .join(df_rechazos, ['driver_id'], 'left')\
    .withColumn('strikes',when(col('strikes').isNull(),lit(0)).otherwise(col('strikes')))\
    .withColumn('dias_antiguedad', when(col('dias_antiguedad').isNull(),lit(0)).otherwise(col('dias_antiguedad')))\
    .withColumn('recharges',when(col('recharges').isNull(),lit(0)).otherwise(col('recharges')))\
    .withColumn('days_with_recharges',when(col('days_with_recharges').isNull(),lit(0)).otherwise(col('days_with_recharges')))\
    .withColumn('total_recharges',when(col('total_recharges').isNull(),lit(0)).otherwise(col('total_recharges')))

# COMMAND ----------

df_workers = datalake.dataframe2tempdataset(dataframe=df_workers, namespace='latam', dataset='wallet_recharges_drivers' )

# COMMAND ----------

df_workers = datalake.read_tempdataset(namespace='latam', dataset='wallet_recharges_drivers' )

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Rules

# COMMAND ----------

# MAGIC %md
# MAGIC ### Escenarios
# MAGIC Drivers de ualquier segmentación con al menos una recarga en los últimos 30 días y...  
# MAGIC Escenario 1: Saldo promedio > 0 en los últimos 30 dias  
# MAGIC Escenario 2: Saldo promedio > -10.000 en los últimos 30 dias  
# MAGIC Escenario 3: Saldo promedio > -50.000 en los últimos 30 dias  
# MAGIC 
# MAGIC ### Escenario por ciudad
# MAGIC Escenario 1:  ['CALI','BUCARAMANGA','SANTA MARTA','VALLEDUPAR','MONTERIA','ARMENIA','GIRON','PALMIRA','CHIA','IBAGUE','PIEDECUESTA','TUNJA','POPAYAN',            'NEIVA','BARRANCABERMEJA','PASTO']  
# MAGIC Escenario 2: ['BARRANQUILLA','MANIZALES','PEREIRA','CARTAGENA','VILLAVICENCIO','GAIRA','CUCUTA']  
# MAGIC Escenario 3: ['MEDELLIN','BOGOTA','BELLO','SOACHA']

# COMMAND ----------

CITIES_S1 = ['BOGOTA', 'CALI', 'BARRANQUILLA', 'MEDELLIN', 'SANTA MARTA', 'VALLEDUPAR', 'MONTERIA', 'PALMIRA','MANIZALES', 'SOLEDAD']

# CITIES_S2 = ['BOGOTA', 'CALI', 'BARRANQUILLA','SANTA MARTA', 'VALLEDUPAR', 'MONTERIA', 'PALMIRA','MANIZALES', 'SOLEDAD']


# CITIES_EXP = ['BELLO', 'CALI', 'MANIZALES']

# COMMAND ----------

df_cities = df_workers\
    .select('logistic_region')\
    .dropDuplicates()\
    .withColumn('final_setting',when(col('logistic_region').isin(CITIES_S1),lit('setting 1')))
#                                 .when(col('logistic_region').isin(CITIES_S2),lit('setting 2')))\

# df_cities = df_workers\
#     .select('logistic_region')\
#     .dropDuplicates()\
#     .withColumn('final_setting',when(col('logistic_region').isin(CITIES_EXP),lit('setting')))\

# COMMAND ----------

# df_workers_rules = df_workers\
#   .withColumn('rule_1',when(col('avg_wallet')>=0,lit(1)).otherwise(lit(0)))\
#   .withColumn('rule_2',when(col('strikes')==0,lit(1)).otherwise(lit(0)))\
#   .withColumn('rule_3',when(col('dias_antiguedad')>30,lit(1)).otherwise(lit(0)))\
#   .withColumn('rule_4',when(col('segmentation_def').isin('Engaged','Performer','Bad', 'Canceler'),lit(1)).otherwise(lit(0)))\
#   .withColumn('rule_5',when(col('avg_recharge')>=30000,lit(1)).otherwise(lit(0)))\
#   .withColumn('rule_6',when(col('prop_declined_orders')<=0.2,lit(1)).otherwise(lit(0)))\
#   .withColumn('rule_7',when(col('online_hours')>=30,lit(1)).otherwise(lit(0)))\
#   .withColumn('rule_8',when(col('online_hours')>=10,lit(1)).otherwise(lit(0)))\
#   .withColumn('rule_9', when(col('orders')>=30,lit(1)).otherwise(lit(0)))\
#   .withColumn('rule_10', when(col('orders')>=15,lit(1)).otherwise(lit(0)))\
#   .withColumn('setting_1', when(col('rule_1')+col('rule_2')+col('rule_3')+col('rule_5')+col('rule_6')+col('rule_7')+col('rule_9') == 7,lit(1)).otherwise(lit(0)))\
#   .withColumn('setting_2', when(col('rule_1')+col('rule_2')+col('rule_3')+col('rule_5')+col('rule_6')+col('rule_8')+col('rule_10') == 7,lit(1)).otherwise(lit(0)))\
#   .withColumn('final_setting',when((col('logistic_region').isin(CITIES_S1))&(col('setting_1')==1),lit(1))\
#                                 .when((col('logistic_region').isin(CITIES_S2))&(col('setting_2')==1),lit(1))\
#                                 .otherwise(lit(0)))

# COMMAND ----------

df_workers_rules = df_workers\
  .withColumn('rule_1',when(col('avg_wallet')>=0,lit(1)).otherwise(lit(0)))\
  .withColumn('rule_2',when(col('strikes')==0,lit(1)).otherwise(lit(0)))\
  .withColumn('rule_3',when(col('dias_antiguedad')>30,lit(1)).otherwise(lit(0)))\
  .withColumn('rule_5',when(col('avg_recharge')>=15000,lit(1)).otherwise(lit(0)))\
  .withColumn('rule_6',when(col('prop_declined_orders')<=0.3,lit(1)).otherwise(lit(0)))\
  .withColumn('rule_9', when(col('orders')>=20,lit(1)).otherwise(lit(0)))\
  .withColumn('rule_10', when(col('orders')>=15,lit(1)).otherwise(lit(0)))\
  .withColumn('setting_1', when(col('rule_1')+col('rule_2')+col('rule_3')+col('rule_5')+col('rule_6')+col('rule_10') == 6,lit(1)).otherwise(lit(0)))\
  .withColumn('final_setting',when((col('logistic_region').isin(CITIES_S1))&(col('setting_1')==1),lit(1))\
                                .otherwise(lit(0)))

# +col('rule_2')

# COMMAND ----------

# display(df_workers_rules)

# COMMAND ----------

etl.dataframe2sandbox(
   dataframe=df_workers_rules,
   namespace='latam',
   dataset='co_driver_nb_features',
)

# COMMAND ----------

# DBTITLE 1,Distribution drivers
# display(df_workers_rules\
#   .select(count('driver_id').alias('drivers'),
#           round(mean('setting_1'),2).alias('setting_1_perc_drivers'),
#           round(mean('final_setting'),2).alias('perc_drivers_def')
#          )

# COMMAND ----------

# DBTITLE 1,Distribution by segment
# display(df_workers_rules\
#   .groupBy('segmentation_def')\
#   .agg(count('driver_id').alias('drivers'),
#           round(mean('setting_1'),2).alias('setting_1_perc_drivers'),
#           round(mean('final_setting'),2).alias('perc_drivers_def')
#          ))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Distribution orders

# COMMAND ----------

total_orders = orders\
  .where("date_partition >= date('{}') and date_partition <= date('{}')".format(START_DATE, END_DATE))\
  .withColumn('cash_orders',when(col('payment_type')=='MONEY',lit(1)).otherwise(lit(0)))\
  .select(count('order_number').alias('total_orders'),
          sum('cash_orders').alias('total_cash_orders'))

distr_orders = orders\
  .where("date_partition >= date('{}') and date_partition <= date('{}')".format(START_DATE, END_DATE))\
  .join(df_workers_rules\
          .select('driver_id','setting_1','final_setting'),
        ['driver_id'],'left')\
  .fillna(0,['setting_1','final_setting'])\
  .withColumn('cash_orders',when(col('payment_type')=='MONEY',lit(1)).otherwise(lit(0)))

# COMMAND ----------

for i,setting in [(1,'setting_1'),(2,'final_setting')]:
  df_temp = distr_orders\
    .groupBy(col(setting).alias('selected'))\
    .agg(count('order_number').alias('orders'),
         sum('cash_orders').alias('cash_orders'))\
    .join(total_orders)\
    .withColumn('perc_orders',round(col('orders')/col('total_orders'),2))\
    .withColumn('perc_cash_orders',round(col('cash_orders')/col('total_cash_orders'),2))\
    .drop('total_orders','total_cash_orders')\
    .withColumn('setting',lit(setting))
  
  if i == 1:
    distr_orders_settings = df_temp
  else:
    distr_orders_settings = distr_orders_settings\
        .unionAll(df_temp)

# COMMAND ----------

# DBTITLE 1,Distribution orders
# display(distr_orders_settings.filter('selected==1').select('setting','perc_orders','perc_cash_orders'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Distribution by logistic region

# COMMAND ----------

total_orders_region = orders\
  .where("date_partition >= date('{}') and date_partition <= date('{}')".format(START_DATE, END_DATE))\
  .withColumn('cash_orders',when(col('payment_type')=='MONEY',lit(1)).otherwise(lit(0)))\
  .groupBy('logistic_region')\
  .agg(count('order_number').alias('total_orders'),
       sum('cash_orders').alias('total_cash_orders'))

# COMMAND ----------

orders_drivers = orders\
  .where("date_partition >= date('{}') and date_partition <= date('{}')".format(START_DATE, END_DATE))\
  .withColumn('cash_orders',when(col('payment_type')=='MONEY',lit(1)).otherwise(lit(0)))\
  .groupBy('driver_id')\
           .agg(count('order_number').alias('orders'),
                sum('cash_orders').alias('cash_orders'))

# COMMAND ----------

def summary_setting_region(setting):
  df_results = df_workers_rules\
    .drop('orders')\
    .select('logistic_region','driver_id',
            col(setting).alias('selected'))\
    .join(orders_drivers,'driver_id','left')\
    .withColumn('orders_selected',col('orders')*col('selected'))\
    .withColumn('cash_orders_selected',col('cash_orders')*col('selected'))\
    .groupBy('logistic_region')\
        .agg(count('driver_id').alias('drivers'),
             round(mean('selected'),2).alias('perc_drivers'),
             sum('selected').alias('selected_drivers'),
             sum('orders_selected').alias('orders'),
             sum('cash_orders_selected').alias('cash_orders'))\
    .join(total_orders_region,['logistic_region'])\
        .withColumn('perc_orders',round(col('orders')/col('total_orders'),2))\
        .withColumn('perc_cash_orders',round(col('cash_orders')/col('total_cash_orders'),2))\
    .orderBy(col('orders').desc())\
    .select('logistic_region','drivers','perc_drivers','selected_drivers','perc_orders','perc_cash_orders')
  
  return df_results

# COMMAND ----------

# DBTITLE 1,Escenario 1
display(summary_setting_region('setting_1').where(col('logistic_region').isin('BARRANQUILLA', 'BOGOTA', 'CALI', 'MEDELLIN', 'SANTA MARTA', 'MANIZALES', 'MONTERIA', 'VALLEDUPAR', 'SOLEDAD', 'PALMIRA')))

# COMMAND ----------

# DBTITLE 1,Escenario 2
# display(summary_setting_region('setting_2').where(col('logistic_region').isin('BARRANQUILLA', 'BOGOTA', 'CALI', 'SANTA MARTA', 'MANIZALES', 'MONTERIA', 'VALLEDUPAR', 'SOLEDAD', 'PALMIRA')))

# COMMAND ----------

# DBTITLE 1,Escenario 3
# display(summary_setting_region('setting_3'))

# COMMAND ----------

# DBTITLE 1,Escenario 4
# display(summary_setting_region('setting_4'))

# COMMAND ----------

# DBTITLE 1,Escenario final
display(summary_setting_region('final_setting').join(df_cities,['logistic_region'],'left')\
                                               .where(col('logistic_region').isin('BARRANQUILLA', 'BOGOTA', 'CALI', 'MEDELLIN', 'SANTA MARTA', 'MANIZALES', 'MONTERIA', 'VALLEDUPAR', 'SOLEDAD', 'PALMIRA')))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final table

# COMMAND ----------

drivers_nb = segmentation\
    .withColumnRenamed('segment','segmentation_def')\
    .withColumnRenamed('segmentation_date','date')\
    .join(df_workers_rules\
              .select('driver_id',col('final_setting').alias('selected')),
          ['driver_id'],'left')\
    .withColumn('nb',when(col('selected')==1,'Prueba').otherwise(''))\
    .withColumn('segment_nb',concat(col('segmentation_def'),col('nb')))\
    .withColumn('date', current_date())\
    .select('driver_id','logistic_region','date','segmentation_def','selected','segment_nb')

# COMMAND ----------

drivers_nb.display()

# COMMAND ----------

display(segmentation)

# COMMAND ----------

# DBTITLE 1,Drivers seleccionados escenario final
display(drivers_nb.select(sum('selected')))

# COMMAND ----------

display(drivers_nb)

# COMMAND ----------

from ifood_databricks.toolbelt.data_quality_validator import ifoodDataQualityValidator

df = datalake.dataframe2tempdataset(dataframe=drivers_nb, 
                                    namespace='latam',
                                    dataset='co_driver_segmentation_nb')

validator = ifoodDataQualityValidator(df,
                                      dataset='latam_sandbox.co_driver_segmentation_nb',
                                      displayName='co_driver_segmentation_nb',
                                      slackUsername="@daniel.gallo")

check = validator.hasUniqueKey('driver_id')

validator.run(check)

if not validator.allConstraintsSatisfied():
  raise Exception("Validation failed!")

# COMMAND ----------

display(df.select(sum('selected')))

# COMMAND ----------

display(df.where(col('selected')==1))

# COMMAND ----------

display(df.where(col('selected')==1).groupBy('logistic_region').agg(sum('selected')))

# COMMAND ----------

df.count()

# COMMAND ----------

from ifood_databricks import datalake, etl

etl.dataframe2sandbox(
   dataframe=df,
   namespace='latam',
   dataset='co_driver_nb_october',
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Tabla final de segmentación guardada en latam_sandbox.co_driver_segmentation_nb!!!

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from latam_sandbox.co_driver_nb_october where selected == 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(selected) from latam_sandbox.co_driver_segmentation_nb_new_test_agosto

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(selected) from latam_sandbox.co_driver_segmentation_nb_new_test_agosto

# COMMAND ----------

# MAGIC %sql
# MAGIC select date from latam_sandbox.co_driver_segmentation_nb_new_test_agosto group by 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from latam_curated.co_drivers_fleet_segmentation
# MAGIC where date = '2022-09-20'

# COMMAND ----------


