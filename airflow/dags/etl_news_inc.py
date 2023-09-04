import pandas as pd
from airflow import DAG
# from airflow.operators.sql import BranchSQLOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.sensors.filesystem import FileSensor
from airflow.hooks.base_hook import BaseHook
from airflow.utils.trigger_rule import TriggerRule
from airflow import settings
from airflow.models import Connection
from airflow.models import Variable
from datetime import datetime, timedelta
from datetime import date
from sqlalchemy import create_engine
import requests
import psycopg2
import feedparser
import os


###################################
### Set Variables & Connections ###

Variable.setdefault(key='etl_news_source_1', default='https://lenta.ru/rss/')
Variable.setdefault(key='etl_news_source_2', default='https://www.vedomosti.ru/rss/news')
Variable.setdefault(key='etl_news_source_3', default='https://tass.ru/rss/v2.xml')

Variable.setdefault(key='raw_data_path', default='/opt/airflow/raw_data')
Variable.setdefault(key='raw_data_folder_name', default='news')

Variable.setdefault(key='db_name', default='db')
Variable.setdefault(key='raw_store_name', default='raw_store')
Variable.setdefault(key='core_store_name', default='core_store')
Variable.setdefault(key='mart_store_name', default='mart_store')

###################################
### Get Variables & Connections ###

try:
    store_connection_params = BaseHook.get_connection('connection_db')
except:
    conn = Connection(
        conn_id='connection_db',
        conn_type='postgres',
        host='db',
        login='postgres',
        password='password',
        schema='postgres',
        port=5432
    )
    session = settings.Session()
    session.add(conn)
    session.commit()

    store_connection_params = BaseHook.get_connection('connection_db')

db_name = Variable.get('db_name')
raw_store_name = Variable.get('raw_store_name')
core_store_name = Variable.get('core_store_name')
mart_store_name = Variable.get('mart_store_name')

raw_data_path = Variable.get('raw_data_path')
raw_data_folder_name = Variable.get('raw_data_folder_name')

data_sources = {
    'lenta_ru': Variable.get('etl_news_source_1'),
    'vedomosti_ru': Variable.get('etl_news_source_2'),
    'tass_ru': Variable.get('etl_news_source_3')
}


def psycopg2_connection(store_connection_params):

    try:
        store_connection = psycopg2.connect(
            dbname=store_connection_params.schema,
            user=store_connection_params.login,
            password=store_connection_params.password,
            host=store_connection_params.host,
            port=store_connection_params.port
        )

        return store_connection

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def fn_create_schema(db_name, schema_name, store_connection):

    store_cursor = store_connection.cursor()

    store_cursor.execute(f"""
            CREATE SCHEMA IF NOT EXISTS {schema_name};
        """)
    store_connection.commit()


def fn_load_data_to_folder(url, path, folder, file):
    response = requests.get(url)
    with open(f'{path}/{folder}/{file}', 'wb') as f:
        f.write(response.content)


def fn_load_data_file_to_db(data_file, store_connection_params, db_name, schema_name, table_name):

  data_feed = feedparser.parse(data_file)
  data_to_df = []

  for el in data_feed.entries:
    
    if datetime.strptime(el['published'], '%a, %d %b %Y %H:%M:%S %z').date().strftime('%Y-%m-%d') == (date.today() - timedelta(days=1)).strftime('%Y-%m-%d'):
      row_date = datetime.strptime(el['published'], '%a, %d %b %Y %H:%M:%S %z').date().strftime('%Y-%m-%d')
    
      for tag in el['tags']:
        spl_tags = tag['term'].split(" / ")
        for spl_tag in spl_tags:
          new_el = {}
          new_el['title'] = el.get('title', '')
          new_el['link'] = el.get('link', '')
          new_el['id'] = el.get('id', '')
          new_el['summary'] = el.get('summary', '')
          new_el['tags'] = spl_tag
          new_el['source'] = table_name
          new_el['published'] = row_date
          data_to_df.append(new_el)

  if len(data_to_df) > 0:

    try:
      engine = create_engine(f'postgresql+psycopg2://{store_connection_params.login}:{store_connection_params.password}@{store_connection_params.host}/{store_connection_params.schema}')
    except (Exception, psycopg2.DatabaseError) as error:
      print(error)

    df = pd.DataFrame(data_to_df)

    df.to_sql(f'news_df_{table_name}', schema=schema_name, con=engine, if_exists='replace', index=False)


def def_get_raw_tables(store_name, store_connection_params, **kwargs):

    try:
      engine = create_engine(f'postgresql+psycopg2://{store_connection_params.login}:{store_connection_params.password}@{store_connection_params.host}/{store_connection_params.schema}')
    except (Exception, psycopg2.DatabaseError) as error:
      print(error)
    
    df = pd.read_sql("""
      SELECT table_name FROM information_schema.tables
      WHERE table_schema = 'raw_store'
    """, engine)

    raw_news_table_names = []
    for col_name, names in df.items():
      for name in names:
        print(name)
        if name.startswith('news_df_'):
          raw_news_table_names.append(name)


    ti = kwargs['ti']
    ti.xcom_push(key=f"raw_tables", value=raw_news_table_names)

def def_raw_tables_to_core(db_name, store_connection, **kwargs):

     ti = kwargs['ti']
     tables = ti.xcom_pull(key='raw_tables')

     for news_table in tables:

      store_cursor = store_connection.cursor()
      store_cursor.execute(f"""
          INSERT INTO core_store.news (url, category, published, source)
            select
                id,
                tags,
                published,
                source
            from raw_store.{news_table} ON CONFLICT DO NOTHING
          """) 
      store_connection.commit()


############
### DAGs ###

etl_news_dag = DAG(
    dag_id='etl_news_inc',
    start_date=datetime(2023, 8, 21),
    schedule_interval='@once'
)



## start
task_start = DummyOperator(task_id='start', dag=etl_news_dag)



# group_create_store
with TaskGroup(group_id='group_create_store', dag=etl_news_dag) as group_create_store:

  task_group_start = EmptyOperator(task_id="group_create_store_start", dag=etl_news_dag)

  task_group_end = EmptyOperator(
        task_id='group_create_store_end',
        trigger_rule=TriggerRule.ALL_DONE, 
        dag=etl_news_dag
    )

  task_create_raw_store = PythonOperator(
          task_id='create_raw_store',
          python_callable=fn_create_schema,
          op_kwargs={'store_connection': psycopg2_connection(store_connection_params),
                    'db_name': db_name,
                    'schema_name': raw_store_name
                    },
          provide_context=True,
          dag=etl_news_dag
      )

  task_create_core_store = PythonOperator(
          task_id='create_core_store',
          python_callable=fn_create_schema,
          op_kwargs={'store_connection': psycopg2_connection(store_connection_params),
                    'db_name': db_name,
                    'schema_name': core_store_name
                    },
          provide_context=True,
          dag=etl_news_dag
      )

  task_create_mart_store = PythonOperator(
          task_id='create_mart_store',
          python_callable=fn_create_schema,
          op_kwargs={'store_connection': psycopg2_connection(store_connection_params),
                    'db_name': db_name,
                    'schema_name': mart_store_name
                    },
          provide_context=True,
          dag=etl_news_dag
      )
  
  task_group_start >> task_create_raw_store >> task_group_end
  task_group_start >> task_create_core_store >> task_group_end
  task_group_start >> task_create_mart_store >> task_group_end



## group_fs
with TaskGroup(group_id='group_fs', dag=etl_news_dag) as group_fs:

  task_group_start = EmptyOperator(task_id="group_fs_start", dag=etl_news_dag)

  task_group_end = EmptyOperator(task_id="group_fs_end", dag=etl_news_dag)

  for key in data_sources:
    task_create_folder = BashOperator(
            task_id=f'create_folder_{key}',
            bash_command='mkdir -p {{ params.PATH }}/{{ params.RAW_FOLDER_NAME }}/initialization_data/{{ params.FOLDER }}',
            params={'PATH': raw_data_path,
                    'RAW_FOLDER_NAME': raw_data_folder_name,
                    'FOLDER': key},
            dag=etl_news_dag
        )

    task_group_start >> task_create_folder >> task_group_end



## group_load
with TaskGroup(group_id='group_load', dag=etl_news_dag) as group_load:

  task_group_start = EmptyOperator(task_id="group_load_start", dag=etl_news_dag)

  task_group_end = EmptyOperator(
        task_id='group_load_end',
        trigger_rule=TriggerRule.ALL_DONE, 
        dag=etl_news_dag
    )

  for key in data_sources:
    task_load_data_to_folder = PythonOperator(
        task_id=f'load_{key}_data_to_folder',
        python_callable=fn_load_data_to_folder,
        op_kwargs={'url': data_sources[key],
                  'path': raw_data_path,
                  'folder': f'{raw_data_folder_name}/initialization_data/{key}',
                  'file': f'{key}.xml'
                  },
        dag=etl_news_dag
    )

    task_group_start >> task_load_data_to_folder >> task_group_end



## group_load_to_db
with TaskGroup(group_id='group_load_to_db', dag=etl_news_dag) as group_load_to_db:

  task_group_start = EmptyOperator(task_id="group_load_start", dag=etl_news_dag)

  task_group_end = EmptyOperator(task_id="group_load_end", dag=etl_news_dag)

  for folder in os.listdir(f"{raw_data_path}/{raw_data_folder_name}/initialization_data"):
    for data_file_name in os.listdir(f"{raw_data_path}/{raw_data_folder_name}/initialization_data/{folder}"):
      # data_file = os.path.join(f"{raw_data_path}/{raw_data_folder_name}/initialization_data/{folder}", data_file_name)

      task_check_folder_include_data = FileSensor(
        fs_conn_id='filepath',
        task_id=f'wait_file_{folder}',
        filepath=f'{raw_data_path}/{raw_data_folder_name}/initialization_data/{folder}/{data_file_name}',
        dag=etl_news_dag
      )

      task_load_data_file_to_db = PythonOperator(
        task_id=f'load_{folder}_from_initialization_data_to_db',
        python_callable=fn_load_data_file_to_db,
        op_kwargs={'data_file': os.path.join(f"{raw_data_path}/{raw_data_folder_name}/initialization_data/{folder}", data_file_name),
                  'store_connection_params': store_connection_params,
                  'db_name': db_name,
                  'schema_name': raw_store_name,
                  'table_name': folder
                  },
        dag=etl_news_dag
      )

      task_group_start >> task_check_folder_include_data >> task_load_data_file_to_db >> task_group_end



## group_create_core_store
with TaskGroup(group_id='group_create_core_store', dag=etl_news_dag) as group_create_core_store:

  task_group_start = EmptyOperator(task_id="group_create_core_store_start", dag=etl_news_dag)

  task_group_end = EmptyOperator(
        task_id='group_create_core_store_end',
        trigger_rule=TriggerRule.ALL_DONE, 
        dag=etl_news_dag
    )

  get_raw_tables = PythonOperator(
        task_id=f'get_raw_tables',
        python_callable=def_get_raw_tables,
        op_kwargs={'store_connection_params': store_connection_params,
                    'store_name': raw_store_name
                  },
        dag=etl_news_dag
  )

  create_core_store_news = PostgresOperator(
        task_id='create_core_store_news',
        postgres_conn_id='connection_db',
        autocommit=True,
        database='postgres',
        sql="""
          CREATE TABLE IF NOT EXISTS core_store.news (
            url varchar,
            category varchar,
            published varchar,
            source varchar,
            PRIMARY KEY (url, category)
          )
        """,
        dag=etl_news_dag
  )

  raw_tables_to_core = PythonOperator(
        task_id=f'raw_tables_to_core',
        python_callable=def_raw_tables_to_core,
        op_kwargs={'store_connection': psycopg2_connection(store_connection_params),
                    'db_name': db_name,
                    'schema_name': mart_store_name
                    },
        dag=etl_news_dag
  )

  task_group_start >> get_raw_tables >> create_core_store_news >> raw_tables_to_core >> task_group_end



# group_create_mart_store
with TaskGroup(group_id='group_create_mart_store', dag=etl_news_dag) as group_create_mart_store:

  task_group_start = EmptyOperator(task_id="group_create_mart_store_start", dag=etl_news_dag)

  task_group_end = EmptyOperator(
        task_id='group_create_mart_store_end',
        trigger_rule=TriggerRule.ALL_DONE, 
        dag=etl_news_dag
    )

  get_raw_tables = PythonOperator(
        task_id=f'get_raw_tables',
        python_callable=def_get_raw_tables,
        op_kwargs={'store_connection_params': store_connection_params,
                    'store_name': raw_store_name
                  },
        dag=etl_news_dag
  )

  create_mart_store_news = PostgresOperator(
        task_id='create_mart_store_news',
        postgres_conn_id='connection_db',
        autocommit=True,
        database='postgres',
        sql="""
          DROP TABLE IF EXISTS mart_store.news;
          CREATE TABLE mart_store.news (
            id SERIAL PRIMARY KEY,
            category VARCHAR,
            source VARCHAR,
            all_source_count_category_news INTEGER,
            source_count_category_news INTEGER,
            all_source_count_category_news_today INTEGER,
            source_count_category_news_today INTEGER,
            avg_news_per_day INTEGER,
            max_news_date DATE,
            amount_news_monday INTEGER,
            amount_news_tuesday INTEGER,
            amount_news_wednesday INTEGER,
            amount_news_thursday INTEGER,
            amount_news_friday INTEGER,
            amount_news_saturday INTEGER,
            amount_news_sunday INTEGER
          )
        """,
        dag=etl_news_dag
  )

  load_data_to_news_mart = PostgresOperator(
      task_id='load_data_to_news_mart',
      postgres_conn_id='connection_db',
      autocommit=True,
      database='postgres',
      sql="""
        WITH temp1 AS (
            select
            category as category,
            source as source,
            count(url) over (partition by category) as all_source_count_category_news,
            count(source) as source_count_category_news,
            to_date(published, 'YYYY-MM-DD') as published,
            count(url) over (partition by category, source, published) as max_news_per_day,
            extract(dow from to_date(published, 'YYYY-MM-DD'))
            from core_store.news n
          group by category, source, n.url, published
        ),
        max_published as (
        select
          category,
          source,
          published,
          max(max_news_per_day) as max_count_news,
          max(max_news_per_day) over (partition by category, source)
        from temp1
        group by category, source, published, temp1.max_news_per_day
        ),
        week_published_news as (
          select
            category,
            amount_news_monday,
            amount_news_tuesday,
            amount_news_wednesday,
            amount_news_thursday,
            amount_news_friday,
            amount_news_saturday,
            amount_news_sunday
          from (
            select 
              category,
              count(source_count_category_news) FILTER (WHERE day_of_week = 1) over (partition by category) as amount_news_monday,
              count(source_count_category_news) FILTER (WHERE day_of_week = 2) over (partition by category) as amount_news_tuesday,
              count(source_count_category_news) FILTER (WHERE day_of_week = 3) over (partition by category) as amount_news_wednesday,
              count(source_count_category_news) FILTER (WHERE day_of_week = 4) over (partition by category) as amount_news_thursday,
              count(source_count_category_news) FILTER (WHERE day_of_week = 5) over (partition by category) as amount_news_friday,
              count(source_count_category_news) FILTER (WHERE day_of_week = 6) over (partition by category) as amount_news_saturday,
              count(source_count_category_news) FILTER (WHERE day_of_week = 7) over (partition by category) as amount_news_sunday 
            from (
              select
                *,
                extract(dow from published) as day_of_week
              from temp1
            ) as tmp_1
          ) as tmp_2
          group by category,
            amount_news_monday,
            amount_news_tuesday,
            amount_news_wednesday,
            amount_news_thursday,
            amount_news_friday,
            amount_news_saturday,
            amount_news_sunday 	
        ),
        temp2 as (
          select
            category,
            source,
            all_source_count_category_news,
            count(source_count_category_news) as source_count_category_news
          from temp1
          group by category, source, all_source_count_category_news, source_count_category_news
        ), 
        temp3 as (
          select
            category,
            source,
            all_source_count_category_news,
            count(source_count_category_news) as source_count_category_news
          from temp1
          where published = DATE(Now())
          group by category, source, all_source_count_category_news, source_count_category_news
        ),
        mart as (
        select
          temp2.category,
          temp2.source,
          temp2.all_source_count_category_news as all_source_count_category_news,
          temp2.source_count_category_news as source_count_category_news,
          temp3.all_source_count_category_news as all_source_count_category_news_today,
          temp3.source_count_category_news as source_count_category_news_today,
          temp2.all_source_count_category_news / count(distinct(temp1.published)) as avg_news_per_day,
          max_published.published as max_news_date,
          week_published_news.amount_news_monday,
          week_published_news.amount_news_tuesday,
          week_published_news.amount_news_wednesday,
          week_published_news.amount_news_thursday,
          week_published_news.amount_news_friday,
          week_published_news.amount_news_saturday,
          week_published_news.amount_news_sunday
        from temp2
        left join temp3 on temp2.category = temp3.category and temp2.source = temp3.source
        left join temp1 on temp1.category = temp2.category
        left join max_published on max_published.category = temp2.category and max_published.source = temp2.source and max_published.max_count_news = max_published.max
        left join week_published_news on week_published_news.category = temp2.category
        group by temp2.category, 
          temp2.source, 
          temp2.all_source_count_category_news, 
          temp2.source_count_category_news, 
          temp3.all_source_count_category_news, 
          temp3.source_count_category_news,
          max_published.published,
          amount_news_monday,
          amount_news_tuesday,
          amount_news_wednesday,
          amount_news_thursday,
          amount_news_friday,
          amount_news_saturday,
          amount_news_sunday
        )
        insert into mart_store.news(
          category,
          source,
          all_source_count_category_news,
          source_count_category_news,
          all_source_count_category_news_today,
          source_count_category_news_today,
          avg_news_per_day,
          max_news_date,
          amount_news_monday,
          amount_news_tuesday,
          amount_news_wednesday,
          amount_news_thursday,
          amount_news_friday,
          amount_news_saturday,
          amount_news_sunday
        )
        select
          category,
          source,
          all_source_count_category_news,
          source_count_category_news,
          all_source_count_category_news_today,
          source_count_category_news_today,
          avg_news_per_day,
          max(max_news_date) as max_news_date,
          amount_news_monday,
          amount_news_tuesday,
          amount_news_wednesday,
          amount_news_thursday,
          amount_news_friday,
          amount_news_saturday,
          amount_news_sunday
        from mart
        group by
          category,
          source,
          all_source_count_category_news,
          source_count_category_news,
          all_source_count_category_news_today,
          source_count_category_news_today,
          avg_news_per_day,
          amount_news_monday,
          amount_news_tuesday,
          amount_news_wednesday,
          amount_news_thursday,
          amount_news_friday,
          amount_news_saturday,
          amount_news_sunday
              """,
      dag=etl_news_dag
  )

  task_group_start >> get_raw_tables >> create_mart_store_news >> load_data_to_news_mart >> task_group_end

task_start >> group_create_store >> group_fs >> group_load >>  group_load_to_db >> group_create_core_store >> group_create_mart_store
