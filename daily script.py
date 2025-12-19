from awsglue import DynamicFrame
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import functions as f
from pyspark.sql.window import Window
from datetime import datetime
import sys, psycopg2


args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'rs_connection',
    'temp_dir',
    'schema',
    'rs_host',
    'rs_port',
    'rs_user'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

TEMP_DIR = args['temp_dir']
SCHEMA = args['schema']
RS_CONN = args['rs_connection']

conn = psycopg2.connect(
    host = args['rs_host'],
    port = args['rs_port'],
    database = SCHEMA,
    user = args['rs_user'],
    password = 'GVSBWCWUr1BCnlnwqXUk'
)
conn.autocommit = False
cursor = conn.cursor()

def get_redshift_table_as_df(schema: str, table: str, temp_dir: str, connection: str):
    """get the redshift table as a spark df"""
    dyn_frame = glueContext.create_dynamic_frame.from_options(
        connection_type = "redshift",
        connection_options = {
            "dbtable": f"{schema}.{table}",
            "redshiftTmpDir": temp_dir,
            "useConnectionProperties": "true",
            "connectionName": connection
        }
    )
    return dyn_frame.toDF()

def calculate_num_den(df):
    modified_df = (df
        .withColumn('gross_margin_pct',
            f.when(f.col('total_sales_amt') == 0, 0)
             .otherwise(f.round((f.col('total_sales_amt') - f.col('total_cogs')) * 100 / f.col('total_sales_amt'), 2))
        )
        .withColumn('stores_gross_margin_amt',
            f.round(f.col('stores_sales_amt') - f.col('total_cogs_stores'), 2)
        )
        .withColumn('stores_gross_margin_pct',
            f.when(f.col('stores_sales_amt') == 0, 0)
             .otherwise(f.round((f.col('stores_sales_amt') - f.col('total_cogs_stores')) * 100 / f.col('stores_sales_amt'), 2))
        )
        .withColumn('initial_markup_pct',
            f.when(f.col('all_promo_exc_ctgs') == 0, 0)
             .otherwise(f.round((f.col('all_promo_exc_ctgs') - f.col('all_cogs_exc_ctgs')) * 100 / f.col('all_promo_exc_ctgs'), 2))
        )
        .withColumn('item_discount_pct',
            f.when(f.col('all_promo_exc_ctgs') == 0, 0)
             .otherwise(f.round((f.col('all_promo_exc_ctgs') - f.col('all_sales_exc_ctgs')) * 100 / f.col('all_promo_exc_ctgs'), 2))
        )
        .withColumn('orders_with_discount_pct',
            f.when(f.col('unique_cust_count') == 0, 0)
             .otherwise(f.round(f.col('discounted_cust_count') * 100.00 / f.col('unique_cust_count'), 2))
        )
        .withColumn('effective_margin_pct',
            f.when(f.col('total_sales_amt') == 0, 0)
             .otherwise(f.round((f.col('total_sales_amt') - f.col('total_cogs') - f.col('total_financing_cost'))
                        * 100 / f.col('total_sales_amt'), 2))
        )
        .withColumn('financing_cost_pct',
            f.when(f.col('total_sales_amt') == 0, 0)
             .otherwise(f.round(f.col('total_financing_cost') * 100 / f.col('total_sales_amt'), 2))
        )
        .withColumn('sales_per_guest',
            f.when(f.col('traffic') == 0, 0)
             .otherwise(f.round(f.col('total_sales_amt') / f.col('traffic'), 2))
        )
        .withColumn('close_rate_pct',
            f.when(f.col('traffic') == 0, 0)
             .otherwise(f.round(f.col('cust_count_exc_ctgs') * 100.00 / f.col('traffic'), 2))
        )
        .withColumn('avg_ticket_amt',
            f.when(f.col('cust_count_exc_ctgs') == 0, 0)
             .otherwise(f.round(f.col('sales_exc_ctgs') / f.col('cust_count_exc_ctgs'), 2))
        )
        .withColumn('items_per_ticket',
            f.when(f.col('cust_count_exc_ctgs') == 0, 0)
             .otherwise(f.round(f.col('qty_exc_ctgs') / f.col('cust_count_exc_ctgs'), 2))
        )
        .withColumn('avg_sales_price',
            f.when(f.col('qty_exc_ctgs') == 0, 0)
             .otherwise(f.round(f.col('sales_exc_ctgs') / f.col('qty_exc_ctgs'), 2))
        )
        .withColumn('carts_built_pct',
            f.when(f.col('traffic') == 0, 0)
             .otherwise(f.round(f.col('carts_count') * 100.00 / f.col('traffic'), 2))
        )
        .withColumn('sales_per_hour',
            f.when(f.col('paid_hours') == 0, 0)
             .otherwise(f.round(f.col('total_sales_amt') / f.col('paid_hours'), 2))
        )
        .withColumn('financed_sales_pct',
            f.when(f.col('total_sales_amt') == 0, 0)
             .otherwise(f.round(f.col('total_financed_amt') * 100 / f.col('total_sales_amt'), 2))
        )
        .withColumn('avg_financed_ticket_amt',
            f.when(f.col('customers_financed') == 0, 0)
             .otherwise(f.round(f.col('sales_financed') / f.col('customers_financed'), 2))
        )
        .withColumn('finance_application_pct',
            f.when(f.col('traffic') == 0, 0)
             .otherwise(f.round(f.col('applications_count') * 100.00 / f.col('traffic'), 2))
        )
        .withColumn('furniture_spg',
            f.when(f.col('traffic') == 0, 0)
             .otherwise(f.round(f.col('furniture_sales_amt') / f.col('traffic'), 2))
        )
        .withColumn('furniture_sph',
            f.when(f.col('paid_hours') == 0, 0)
             .otherwise(f.round(f.col('furniture_sales_amt') / f.col('paid_hours'), 2))
        )
        .withColumn('furniture_sales_pct',
            f.when(f.col('total_sales_amt') == 0, 0)
             .otherwise(f.round(f.col('furniture_sales_amt') * 100 / f.col('total_sales_amt'), 2))
        )
        .withColumn('furniture_gross_margin_pct',
            f.when(f.col('furniture_sales_amt') == 0, 0)
             .otherwise(f.round((f.col('furniture_sales_amt') - f.col('furniture_cogs')) * 100 / f.col('furniture_sales_amt'), 2))
        )
        .withColumn('bedding_spg',
            f.when(f.col('traffic') == 0, 0)
             .otherwise(f.round(f.col('bedding_sales_amt') / f.col('traffic'), 2))
        )
        .withColumn('bedding_sph',
            f.when(f.col('paid_hours') == 0, 0)
             .otherwise(f.round(f.col('bedding_sales_amt') / f.col('paid_hours'), 2))
        )
        .withColumn('bedding_sales_pct',
            f.when(f.col('total_sales_amt') == 0, 0)
             .otherwise(f.round(f.col('bedding_sales_amt') * 100 / f.col('total_sales_amt'), 2))
        )
        .withColumn('bedding_gross_margin_pct',
            f.when(f.col('bedding_sales_amt') == 0, 0)
             .otherwise(f.round((f.col('bedding_sales_amt') - f.col('bedding_cogs')) * 100 / f.col('bedding_sales_amt'), 2))
        )
        .withColumn('protection_spg',
            f.when(f.col('traffic') == 0, 0)
             .otherwise(f.round(f.col('protection_sales_amt') / f.col('traffic'), 2))
        )
        .withColumn('protection_sph',
            f.when(f.col('paid_hours') == 0, 0)
             .otherwise(f.round(f.col('protection_sales_amt') / f.col('paid_hours'), 2))
        )
        .withColumn('protection_sales_pct',
            f.when(f.col('total_sales_amt') == 0, 0)
             .otherwise(f.round(f.col('protection_sales_amt') * 100 / f.col('total_sales_amt'), 2))
        )
        .withColumn('delivery_spg',
            f.when(f.col('traffic') == 0, 0)
             .otherwise(f.round(f.col('delivery_sales_amt') / f.col('traffic'), 2))
        )
        .withColumn('delivery_sales_pct',
            f.when(f.col('total_sales_amt') == 0, 0)
             .otherwise(f.round(f.col('delivery_sales_amt') * 100 / f.col('total_sales_amt'), 2))
        )
        .withColumn('delivered_gross_margin_pct',
            f.when(f.col('sales_delivered') == 0, 0)
             .otherwise(f.round((f.col('sales_delivered') - f.col('cogs_delivered')) * 100 / f.col('sales_delivered'), 2))
        )
        .withColumn('avg_days_to_deliver',
            f.when(f.col('qty_delivered') == 0, 0)
             .otherwise(f.round(f.col('weighted_days') / f.col('qty_delivered'), 2))
        )
    )
    
    return modified_df

def aggregate_by(df, agg_type: str, period: str, is_comp: bool):
    cols = [
        'traffic', 'total_sales_amt', 'stores_sales_amt', 'online_sales_amt', 'omni_sales_amt',
        'total_cogs', 'total_cogs_stores', 'total_tax_amt', 'total_deposit_amt', 'carts_count',
        'applications_count', 'paid_hours', 'bedding_sales_amt', 'protection_sales_amt',
        'furniture_sales_amt', 'furniture_cogs', 'bedding_cogs', 'furniture_financing_cost',
        'bedding_financing_cost', 'delivery_sales_amt', 'cancelled_sales_amt', 'sales_financed',
        'customers_financed', 'total_financed_amt', 'total_financing_cost', 'unique_cust_count',
        'discounted_cust_count', 'all_sales_exc_ctgs', 'all_cogs_exc_ctgs', 'all_promo_exc_ctgs',
        'cust_count_exc_ctgs', 'sales_exc_ctgs', 'qty_exc_ctgs', 'sales_delivered',
        'cogs_delivered', 'qty_delivered', 'weighted_days', 'bedding_sales_delivered'
    ]
    
    if not is_comp:
        agg_part = [f.sum(f.col(c)).alias(c) for c in cols]
    else:
        agg_part = [f.sum(f.when(f.col('ly_date') >= f.col('store_open_date'),
                        f.col(c)).otherwise(0)).alias(c) for c in cols]
    
    group_cols = ['report_date', 'ly_date']
    if period != 'daily': group_cols.append(f'{period}_start')
    
    if agg_type == 'total':
        modified_df = df
    elif agg_type == 'stores':
        modified_df = df.filter(~f.col('profit_center').isin('444', '445'))
    elif agg_type == 'online':
        modified_df = df.filter(f.col('profit_center') == '444')
    elif agg_type == 'omni':
        modified_df = df.filter(f.col('profit_center') == '445')
    else:
        modified_df = df.filter(~f.trim(agg_type).isin('', 'n/a'))
        group_cols.append(agg_type)
    
    modified_df = modified_df.groupBy(*group_cols).agg(*agg_part)
    
    if agg_type in ('region', 'market'):
        modified_df = (modified_df.withColumn('filter_level', f.lit(agg_type.title()))
            .withColumn('segment', f.col(agg_type))
            .withColumn('custom_key', f.concat_ws(';', f.col('report_date'), f.col(agg_type)))
        )
    else:
        modified_df = (modified_df.withColumn('filter_level', f.lit('Aggregated'))
            .withColumn('segment', f.lit(agg_type.title()))
            .withColumn('custom_key', f.concat_ws(';', f.col('report_date'), f.lit(agg_type)))
        )
    
    return modified_df

def full_union(df, period: str, is_comp: bool):
    period_part1 = calculate_num_den(df)
    
    period_total = aggregate_by(df, 'total', period, is_comp)
    period_part2 = calculate_num_den(period_total)
    
    period_stores = aggregate_by(df, 'stores', period, is_comp)
    period_part3 = calculate_num_den(period_stores)
    
    period_online = aggregate_by(df, 'online', period, is_comp)
    period_part4 = calculate_num_den(period_online)
    
    period_omni = aggregate_by(df, 'omni', period, is_comp)
    period_part5 = calculate_num_den(period_omni)
    
    period_region = aggregate_by(df, 'region', period, is_comp)
    period_part6 = calculate_num_den(period_region)
    
    period_market = aggregate_by(df, 'market', period, is_comp)
    period_part7 = calculate_num_den(period_market)
    
    combined_df = (period_part1
        .unionByName(period_part2, True).unionByName(period_part3, True)
        .unionByName(period_part4, True).unionByName(period_part5, True)
        .unionByName(period_part6, True).unionByName(period_part7, True)
    )
    return combined_df

def calculate_ly(df1, df2 = None):
    if df2 is None:
        df2 = df1
        join_condition = (f.col('a.ly_date')==f.col('b.report_date'))
    else:
        join_condition = (f.col('a.report_date')==f.col('b.ny_date'))
    
    ly_cols = [
        'traffic', 'total_sales_amt', 'stores_sales_amt', 'online_sales_amt', 'omni_sales_amt',
        'cancelled_sales_amt', 'gross_margin_pct', 'stores_gross_margin_amt', 'stores_gross_margin_pct',
        'initial_markup_pct', 'item_discount_pct', 'orders_with_discount_pct',
        'effective_margin_pct', 'financing_cost_pct', 'avg_ticket_amt', 'items_per_ticket',
        'avg_sales_price', 'close_rate_pct', 'carts_built_pct', 'sales_per_guest', 'total_financed_amt',
        'financed_sales_pct', 'avg_financed_ticket_amt', 'finance_application_pct', 'sales_per_hour',
        'bedding_sales_amt', 'bedding_spg', 'bedding_sph', 'bedding_sales_pct', 'bedding_gross_margin_pct',
        'bedding_financing_cost', 'furniture_sales_amt', 'furniture_spg', 'furniture_sph',
        'furniture_sales_pct', 'furniture_gross_margin_pct', 'furniture_financing_cost',
        'protection_sales_amt', 'protection_spg', 'protection_sph', 'protection_sales_pct',
        'delivery_sales_amt', 'delivery_spg', 'delivery_sales_pct', 'sales_delivered',
        'avg_days_to_deliver', 'delivered_gross_margin_pct', 'bedding_sales_delivered'
    ]
    
    ly_df = (df1.alias('a').join(df2.alias('b'), join_condition & (f.col('a.segment')==f.col('b.segment')), 'left')
                .select('a.report_date', 'a.app_key', 'a.profit_center', 'a.custom_key', 'a.filter_level',
                    'a.segment', 'a.store_name', 'a.region', 'a.market', *[f.when((f.col(f'b.{c}') == 0) | f.isnull(f'b.{c}')
                    | f.isnull(f'a.{c}'), None).otherwise(f.least(f.lit(1000), f.greatest(f.round((f.col(f'a.{c}') - f.col(f'b.{c}'))
                    * 100 / f.col(f'b.{c}'), 2), f.lit(-1000)))).alias(c) for c in ly_cols])
            )
    
    return ly_df

def to_date(df, period: str, date_col: str):
    cols = [
        'traffic', 'total_sales_amt', 'stores_sales_amt', 'online_sales_amt', 'omni_sales_amt',
        'total_cogs', 'total_cogs_stores', 'total_tax_amt', 'total_deposit_amt', 'carts_count',
        'applications_count', 'paid_hours', 'bedding_sales_amt', 'protection_sales_amt',
        'furniture_sales_amt', 'furniture_cogs', 'bedding_cogs', 'furniture_financing_cost',
        'bedding_financing_cost', 'delivery_sales_amt', 'cancelled_sales_amt', 'sales_financed',
        'customers_financed', 'total_financed_amt', 'total_financing_cost', 'unique_cust_count',
        'discounted_cust_count', 'all_sales_exc_ctgs', 'all_cogs_exc_ctgs', 'all_promo_exc_ctgs',
        'cust_count_exc_ctgs', 'sales_exc_ctgs', 'qty_exc_ctgs', 'sales_delivered',
        'cogs_delivered', 'qty_delivered', 'weighted_days', 'bedding_sales_delivered'
    ]
    
    date_trunc_expr = f.date_trunc(period, f.col(date_col))
    window_spec = (Window.partitionBy(date_trunc_expr, 'store_name').orderBy(date_col)
                    .rowsBetween(Window.unboundedPreceding, Window.currentRow))
    ptd_df = df.select('report_date', 'ly_date', date_trunc_expr.cast('date').alias(f'{period}_start'),
        'app_key', 'profit_center', 'custom_key', 'filter_level', 'segment', 'store_name', 'region',
        'market', *[f.sum(c).over(window_spec).alias(c) for c in cols]
    )
    
    return ptd_df

def save_table(df, schema: str, table: str):
    if table.startswith('ly') or table.startswith('comp'):
        dyn_frame = DynamicFrame.fromDF(df, glueContext)
        pre_action = f'''CREATE TABLE {schema}.{table} (report_date date, app_key bigint, profit_center varchar,
                        custom_key varchar(40), filter_level varchar(40), segment varchar(40), store_name varchar(40),
                        region varchar, market varchar(40), traffic decimal(10, 2), total_sales_amt decimal(10, 2),
                        stores_sales_amt decimal(10, 2), online_sales_amt decimal(10, 2), omni_sales_amt decimal(10, 2),
                        cancelled_sales_amt decimal(10, 2), gross_margin_pct decimal(10, 2), stores_gross_margin_amt decimal(10, 2),
                        stores_gross_margin_pct decimal(10, 2), initial_markup_pct decimal(10, 2), item_discount_pct decimal(10, 2),
                        orders_with_discount_pct decimal(10, 2), effective_margin_pct decimal(10, 2),
                        financing_cost_pct decimal(10, 2), avg_ticket_amt decimal(10, 2), items_per_ticket decimal(10, 2),
                        avg_sales_price decimal(10, 2), close_rate_pct decimal(10, 2), carts_built_pct decimal(10, 2),
                        sales_per_guest decimal(10, 2), total_financed_amt decimal(10, 2), financed_sales_pct decimal(10, 2),
                        avg_financed_ticket_amt decimal(10, 2), finance_application_pct decimal(10, 2),
                        sales_per_hour decimal(10, 2), bedding_sales_amt decimal(10, 2), bedding_spg decimal(10, 2),
                        bedding_sph decimal(10, 2), bedding_sales_pct decimal(10, 2), bedding_gross_margin_pct decimal(10, 2),
                        bedding_financing_cost decimal(10, 2), furniture_sales_amt decimal(10, 2), furniture_spg decimal(10, 2),
                        furniture_sph decimal(10, 2), furniture_sales_pct decimal(10, 2), furniture_gross_margin_pct decimal(10, 2),
                        furniture_financing_cost decimal(10, 2), protection_sales_amt decimal(10, 2), protection_spg decimal(10, 2),
                        protection_sph decimal(10, 2), protection_sales_pct decimal(10, 2), delivery_sales_amt decimal(10, 2),
                        delivery_spg decimal(10, 2), delivery_sales_pct decimal(10, 2), sales_delivered decimal(10, 2),
                        avg_days_to_deliver decimal(10, 2), delivered_gross_margin_pct decimal(10, 2),
                        bedding_sales_delivered decimal(10, 2));
                    '''
    elif 'transpose' in table:
        dyn_frame = DynamicFrame.fromDF(df, glueContext)
        pre_action = f'''CREATE TABLE {schema}.{table} (report_date date, filter_level varchar(40), segment varchar(40),
                        metric varchar(40), column_order int, color varchar(1), daily_ty decimal(21, 2), daily_ly decimal(10, 2),
                        daily_comp decimal(10, 2), wtd_ty decimal(21, 2), wtd_ly decimal(10, 2), wtd_comp decimal(10, 2),
                        mtd_ty decimal(21, 2), mtd_ly decimal(10, 2), mtd_comp decimal(10, 2), ytd_ty decimal(21, 2),
                        ytd_ly decimal(10, 2), ytd_comp decimal(10, 2));
                    '''
    else:
        custom_order = ['report_date', 'ly_date', 'app_key', 'profit_center', 'custom_key', 'filter_level',
                        'segment', 'store_name', 'region', 'market']
        if 'daily' in table:
            period_date = ''
        elif 'wtd' in table:
            period_date = 'week_start date,'
            custom_order.append('week_start')
        elif 'mtd' in table:
            period_date = 'month_start date,'
            custom_order.append('month_start')
        else:
            period_date = 'year_start date,'
            custom_order.append('year_start')
        
        cols_order = [c for c in sorted(df.columns) if c not in custom_order]
        df = df.select(*custom_order, *cols_order)
        dyn_frame = DynamicFrame.fromDF(df, glueContext)
        
        pre_action = f'''CREATE TABLE {schema}.{table} (report_date date, ly_date date, app_key bigint, profit_center varchar,
                        custom_key varchar(40), filter_level varchar(40), segment varchar(40), store_name varchar(40), region varchar(10),
                        market varchar(40), {period_date} all_cogs_exc_ctgs decimal(15, 2), all_promo_exc_ctgs decimal(15, 2),
                        all_sales_exc_ctgs decimal(15, 2), applications_count bigint, avg_days_to_deliver decimal(15, 2),
                        avg_financed_ticket_amt decimal(15, 2), avg_sales_price decimal(15, 2), avg_ticket_amt decimal(15, 2),
                        bedding_cogs decimal(15, 2), bedding_financing_cost decimal(15, 2), bedding_gross_margin_pct decimal(10, 2),
                        bedding_sales_amt decimal(15, 2), bedding_sales_delivered decimal(15, 2), bedding_sales_pct decimal(10, 2),
                        bedding_sph decimal(15, 2), bedding_spg decimal(15, 2), cancelled_sales_amt decimal(15, 2),
                        carts_built_pct decimal(10, 2), carts_count bigint, cogs_delivered decimal(15, 2),
                        close_rate_pct decimal(10, 2), customers_financed bigint, cust_count_exc_ctgs bigint,
                        discounted_cust_count bigint, delivered_gross_margin_pct decimal(10, 2), delivery_sales_amt decimal(15, 2),
                        delivery_sales_pct decimal(10, 2), delivery_spg decimal(15, 2),
                        effective_margin_pct decimal(10, 2), finance_application_pct decimal(10, 2),
                        financed_sales_pct decimal(10, 2), financing_cost_pct decimal(10, 2), furniture_cogs decimal(15, 2),
                        furniture_financing_cost decimal(15, 2), furniture_gross_margin_pct decimal(10, 2),
                        furniture_sales_amt decimal(15, 2), furniture_sales_pct decimal(10, 2), furniture_sph decimal(15, 2),
                        furniture_spg decimal(15, 2), gross_margin_pct decimal(10, 2), initial_markup_pct decimal(10, 2),
                        item_discount_pct decimal(10, 2), items_per_ticket decimal(15, 2), omni_sales_amt decimal(15, 2),
                        online_sales_amt decimal(15, 2), orders_with_discount_pct decimal(10, 2), paid_hours decimal(15, 2),
                        protection_sales_amt decimal(15, 2), protection_sales_pct decimal(10, 2), protection_sph decimal(15, 2),
                        protection_spg decimal(15, 2), qty_delivered decimal(15, 2), qty_exc_ctgs decimal(15, 2),
                        sales_delivered decimal(15, 2), sales_exc_ctgs decimal(15, 2), sales_financed decimal(15, 2),
                        sales_per_guest decimal(15, 2), sales_per_hour decimal(15, 2), --stores_cogs_stores decimal(15, 2),
                        stores_gross_margin_amt decimal(15, 2), stores_gross_margin_pct decimal(10, 2), stores_sales_amt decimal(15, 2),
                        total_cogs decimal(15, 2), total_cogs_stores decimal(15, 2), total_deposit_amt decimal(15, 2),
                        total_financed_amt decimal(15, 2), total_financing_cost decimal(15, 2), total_sales_amt decimal(15, 2),
                        total_tax_amt decimal(15, 2), traffic bigint, unique_cust_count bigint, weighted_days decimal(15, 2)
                    );'''
    
    cursor.execute(f'DROP TABLE IF EXISTS {schema}.{table} CASCADE;')
    conn.commit()
    
    glueContext.write_dynamic_frame.from_options(
        frame = dyn_frame,
        connection_type = 'redshift',
        connection_options = {
            'redshiftTmpDir': TEMP_DIR,
            'useConnectionProperties': 'true',
            'dbtable': f'{schema}.{table}',
            'connectionName': RS_CONN,
            'preactions': f'DROP TABLE IF EXISTS {schema}.{table} CASCADE; {pre_action}'
            # , 'postactions': f'CREATE VIEW users_views.{table}_vw AS SELECT * FROM {schema}.{table};'
        }
    )

def transpose(df, col_name: str):
    base_df = df.filter(f.col('report_date') >= f.date_add(f.current_date(), -91))
    cast_by = 'decimal(21, 2)' if col_name.endswith('ty') else 'decimal(10, 2)'
    mappings = [
        ('Total Sales (inc. Ecom and Omni)', 'total_sales_amt', 1, '1'),
        ('Sales (Stores)', 'stores_sales_amt', 2, '2'),
        ('Sales (Ecom)', 'online_sales_amt', 3, '3'),
        ('Sales (Omni)', 'omni_sales_amt', 4, '2'),
        ('Cancellations (inc. Ecom and Omni)', 'cancelled_sales_amt', 5, '3'),
        ('Gross Margin % (inc. Ecom and Omni)', 'gross_margin_pct', 6, '1'),
        ('Gross Margin (Stores)', 'stores_gross_margin_amt', 7, '2'),
        ('Gross Margin % (Stores)', 'stores_gross_margin_pct', 8, '3'),
        ('IMU', 'initial_markup_pct', 9, '2'),
        ('Item Discount %', 'item_discount_pct', 10, '3'),
        ('% Orders with Discount', 'orders_with_discount_pct', 11, '2'),
        # ('Effective Margin $', 'effective_margin_amt', 12, '3'),
        ('Effective Margin %', 'effective_margin_pct', 12, '3'),
        ('Cost of Financing', 'financing_cost_pct', 13, '2'),
        ('Sales per Guest', 'sales_per_guest', 14, '1'),
        ('Traffic', 'traffic', 15, '2'),
        ('Close Rate', 'close_rate_pct', 16, '3'),
        ('Average Ticket', 'avg_ticket_amt', 17, '2'),
        ('Items per Ticket', 'items_per_ticket', 18, '3'),
        ('Average Selling Price', 'avg_sales_price', 19, '2'),
        ('Carts Built', 'carts_built_pct', 20, '3'),
        ('Sales per Hour', 'sales_per_hour', 21, '2'),
        ('Financed Sales $', 'total_financed_amt', 22, '1'),
        ('Finance % of Sales', 'financed_sales_pct', 23, '2'),
        ('Finance Average Ticket', 'avg_financed_ticket_amt', 24, '3'),
        ('Finance Apps to Traffic', 'finance_application_pct', 25, '2'),
        ('Furniture Sales $', 'furniture_sales_amt', 26, '1'),
        ('Furniture SPG', 'furniture_spg', 27, '2'),
        ('Furniture % of sales', 'furniture_sales_pct', 28, '3'),
        ('Furniture SPH', 'furniture_sph', 29, '2'),
        ('Furniture GM %', 'furniture_gross_margin_pct', 30, '3'),
        ('Furniture COF', 'furniture_financing_cost', 31, '2'),
        ('Bedding Sales $', 'bedding_sales_amt', 32, '1'),
        ('Bedding SPG', 'bedding_spg', 33, '2'),
        ('Bedding % of Sales', 'bedding_sales_pct', 34, '3'),
        ('Bedding SPH', 'bedding_sph', 35, '2'),
        ('Bedding GM %', 'bedding_gross_margin_pct', 36, '3'),
        ('Bedding COF', 'bedding_financing_cost', 37, '2'),
        ('Protection Sales $', 'protection_sales_amt', 38, '1'),
        ('Protection SPG', 'protection_spg', 39, '2'),
        ('Protection % of Sales', 'protection_sales_pct', 40, '3'),
        ('Protection SPH', 'protection_sph', 41, '2'),
        ('Delivery Sales $', 'delivery_sales_amt', 42, '1'),
        ('Delivery SPG', 'delivery_spg', 43, '2'),
        ('Delivery % of sales', 'delivery_sales_pct', 44, '3'),
        ('Total Sales Delivered', 'sales_delivered', 45, '1'),
        ('Bedding Sales Delivered', 'bedding_sales_delivered', 46, '2'),
        ('Gross Margin Delivered %', 'delivered_gross_margin_pct', 47, '3'),
        ('Average Days to Deliver', 'avg_days_to_deliver', 48, '2')
    ]
    stack_expr = ', '.join([f"'{a}', cast({b} as {cast_by}), {c}, '{d}'" for a, b, c, d in mappings])
    
    return base_df.select('report_date', 'filter_level', 'segment', f.expr(f'''stack(
            {len(mappings)}, {stack_expr}) AS (metric, {col_name}, column_order, color)'''))

# ============= Job start =============
job_start_time = datetime.now()
print(f"Job processing started: {job_start_time}")

print('Getting all the required tables.')
report_base_df = get_redshift_table_as_df(SCHEMA, 'daily_report_base', TEMP_DIR, RS_CONN)

store_lookup_df = get_redshift_table_as_df(SCHEMA, 'store_lookup', TEMP_DIR, RS_CONN)

past_year_df = (report_base_df.filter(f.col('report_date') <= f.date_add(f.current_date(), -365))
                .withColumn('ny_date', f.date_add(f.col('report_date'), 364)))



# ============= daily section =============
print('Calculating the Daily section TY values.')
daily_df = full_union(report_base_df, 'daily', False)
save_table(daily_df, SCHEMA, 'daily_report')

print('Calculating the Daily section LY values.')
ly_daily_df = calculate_ly(daily_df)
save_table(ly_daily_df, SCHEMA, 'ly_daily_report')

print('Calculating the Daily section Comp values.')
daily_comp_prep = (report_base_df.alias('a').join(store_lookup_df.alias('b'), (f.col('a.app_key')==f.col('b.app_key'))
                        & (f.col('a.profit_center').cast('int')==f.col('b.pft_ctr')), 'left')
                    .select('a.*', f.coalesce(f.col('grand_open_date'), f.lit('2020-01-01')).cast('date').alias('store_open_date'))
                )
daily_comp_base = full_union(daily_comp_prep, 'daily', True)

comp_daily_df = calculate_ly(daily_comp_base)
save_table(comp_daily_df, SCHEMA, 'comp_daily_report')

# ============= wtd section =============
print('Calculating the WTD section TY values.')
wtd_base = to_date(report_base_df, 'week', 'report_date')

wtd_df = full_union(wtd_base, 'week', False)
save_table(wtd_df, SCHEMA, 'wtd_report')

print('Calculating the WTD section LY values.')
ly_wtd_df = calculate_ly(wtd_df)
save_table(ly_wtd_df, SCHEMA, 'ly_wtd_report')

print('Calculating the WTD section Comp values.')
wtd_comp_prep = (wtd_base.alias('a').join(store_lookup_df.alias('b'), (f.col('a.app_key')==f.col('b.app_key'))
                        & (f.col('a.profit_center').cast('int')==f.col('b.pft_ctr')), 'left')
                    .select('a.*', f.coalesce(f.col('grand_open_date'), f.lit('2020-01-01')).cast('date').alias('store_open_date'))
                )
wtd_comp_base = full_union(wtd_comp_prep, 'week', True)

comp_wtd_df = calculate_ly(wtd_comp_base)
save_table(comp_wtd_df, SCHEMA, 'comp_wtd_report')

# ============= mtd section =============
print('Calculating the MTD section TY values.')
mtd_base1 = to_date(report_base_df, 'month', 'report_date')
mtd_base2 = to_date(past_year_df, 'month', 'ny_date').drop('ny_date')

mtd_df = full_union(mtd_base1, 'month', False)
save_table(mtd_df, SCHEMA, 'mtd_report')

print('Calculating the MTD section LY values.')
mtd_ly_base = (full_union(mtd_base2, 'month', False)
                .withColumn('ny_date', f.date_add(f.col('report_date'), 364)))

ly_mtd_df = calculate_ly(mtd_df, mtd_ly_base)
save_table(ly_mtd_df, SCHEMA, 'ly_mtd_report')

print('Calculating the MTD section Comp values.')
mtd_comp_prep1 = (mtd_base1.alias('a').join(store_lookup_df.alias('b'), (f.col('a.app_key')==f.col('b.app_key'))
                        & (f.col('a.profit_center').cast('int')==f.col('b.pft_ctr')), 'left')
                    .select('a.*', f.coalesce(f.col('grand_open_date'), f.lit('2020-01-01')).cast('date').alias('store_open_date'))
                )
mtd_comp_base1 = full_union(mtd_comp_prep1, 'month', True)

mtd_comp_prep2 = (mtd_base2.alias('a').join(store_lookup_df.alias('b'), (f.col('a.app_key')==f.col('b.app_key'))
                        & (f.col('a.profit_center').cast('int')==f.col('b.pft_ctr')), 'left')
                    .select('a.*', f.coalesce(f.col('grand_open_date'), f.lit('2020-01-01')).cast('date').alias('store_open_date'))
                )
mtd_comp_base2 = (full_union(mtd_comp_prep2, 'month', True)
                    .withColumn('ny_date', f.date_add(f.col('report_date'), 364)))

comp_mtd_df = calculate_ly(mtd_comp_base1, mtd_comp_base2)
save_table(comp_mtd_df, SCHEMA, 'comp_mtd_report')

# ============= ytd section =============
print('Calculating the YTD section TY values.')
ytd_base1 = to_date(report_base_df, 'year', 'report_date')
ytd_base2 = to_date(past_year_df, 'year', 'ny_date').drop('ny_date')

ytd_df = full_union(ytd_base1, 'year', False)
save_table(ytd_df, SCHEMA, 'ytd_report')

print('Calculating the YTD section LY values.')
ytd_ly_base = (full_union(ytd_base2, 'year', False)
                .withColumn('ny_date', f.date_add(f.col('report_date'), 364)))

ly_ytd_df = calculate_ly(ytd_df, ytd_ly_base)
save_table(ly_ytd_df, SCHEMA, 'ly_ytd_report')

print('Calculating the YTD section Comp values.')
ytd_comp_prep1 = (ytd_base1.alias('a').join(store_lookup_df.alias('b'), (f.col('a.app_key')==f.col('b.app_key'))
                        & (f.col('a.profit_center').cast('int')==f.col('b.pft_ctr')), 'left')
                    .select('a.*', f.coalesce(f.col('grand_open_date'), f.lit('2020-01-01')).cast('date').alias('store_open_date'))
                )
ytd_comp_base1 = full_union(ytd_comp_prep1, 'year', True)

ytd_comp_prep2 = (ytd_base2.alias('a').join(store_lookup_df.alias('b'), (f.col('a.app_key')==f.col('b.app_key'))
                        & (f.col('a.profit_center').cast('int')==f.col('b.pft_ctr')), 'left')
                    .select('a.*', f.coalesce(f.col('grand_open_date'), f.lit('2020-01-01')).cast('date').alias('store_open_date'))
                )
ytd_comp_base2 = (full_union(ytd_comp_prep2, 'year', True)
                    .withColumn('ny_date', f.date_add(f.col('report_date'), 364)))

comp_ytd_df = calculate_ly(ytd_comp_base1, ytd_comp_base2)
save_table(comp_ytd_df, SCHEMA, 'comp_ytd_report')

# ============= Transpose logic =============
daily_ty_t = transpose(daily_df, 'daily_ty')
daily_ly_t = transpose(ly_daily_df, 'daily_ly')
daily_comp_t = transpose(comp_daily_df, 'daily_comp')

wtd_ty_t = transpose(wtd_df, 'wtd_ty')
wtd_ly_t = transpose(ly_wtd_df, 'wtd_ly')
wtd_comp_t = transpose(comp_wtd_df, 'wtd_comp')

mtd_ty_t = transpose(mtd_df, 'mtd_ty')
mtd_ly_t = transpose(ly_mtd_df, 'mtd_ly')
mtd_comp_t = transpose(comp_mtd_df, 'mtd_comp')

ytd_ty_t = transpose(ytd_df, 'ytd_ty')
ytd_ly_t = transpose(ly_ytd_df, 'ytd_ly')
ytd_comp_t = transpose(comp_ytd_df, 'ytd_comp')

print('Full joining all the transpose dfs and ordering the columns for proper saving.')

t_join_cols = ['report_date', 'filter_level', 'segment', 'metric', 'column_order', 'color']
t_join_type = 'full'

combined_t = (daily_ty_t.join(daily_ly_t, t_join_cols, t_join_type).join(daily_comp_t, t_join_cols, t_join_type)
                .join(wtd_ty_t, t_join_cols, t_join_type).join(wtd_ly_t, t_join_cols, t_join_type)
                .join(wtd_comp_t, t_join_cols, t_join_type).join(mtd_ty_t, t_join_cols, t_join_type)
                .join(mtd_ly_t, t_join_cols, t_join_type).join(mtd_comp_t, t_join_cols, t_join_type)
                .join(ytd_ty_t, t_join_cols, t_join_type).join(ytd_ly_t, t_join_cols, t_join_type)
                .join(ytd_comp_t, t_join_cols, t_join_type).select(*t_join_cols, 'daily_ty', 'daily_ly', 'daily_comp',
                    'wtd_ty', 'wtd_ly', 'wtd_comp', 'mtd_ty', 'mtd_ly', 'mtd_comp', 'ytd_ty', 'ytd_ly', 'ytd_comp')
            )
save_table(combined_t, SCHEMA, 'combined_transposed_report')

cursor.close()
conn.close()

# ============= Job end =============
job_end_time = datetime.now()
job_duration = job_end_time - job_start_time
print(f'Job completed: {job_end_time} | Run duration: {job_duration}')

job.commit()