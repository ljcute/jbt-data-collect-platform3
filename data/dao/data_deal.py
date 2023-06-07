from db_connection import global_pool
from utils.snowflake_utils import get_id
import pandas as pd
import pandas.io.sql as sqll


def insert_data_collect(record_num, data_text, date, data_type, data_source, start_dt, end_dt, used_time, data_url,
                        data_status, excel_file_path=None):
    conn = global_pool.connection()
    cursor = conn.cursor()
    # 生成雪花id
    log_id = get_id()

    try:
        cursor.execute("""
        insert into t_ndc_data_collect_log(log_id, biz_dt, data_type, data_source, source_doc_url, data_url, record_num
        , data_text, start_dt, end_dt, cost_time, data_status, create_dt, update_dt) 
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s ,%s, %s , %s, now(), now())
        """, [log_id, date, data_type, data_source, excel_file_path, data_url, record_num, data_text, start_dt, end_dt,
              used_time, data_status])

        conn.commit()
    except Exception as es:
        conn.rollback()
        raise Exception(es)
    finally:
        conn.close()


def get_max_biz_dt():
    conn = global_pool.connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            select max(biz_dt) as dt from t_ndc_data_collect_log where data_source = '深圳交易所'
        """)
        result = cursor.fetchall()
        if result:
            return result[0][0].strftime("%Y-%m-%d")
        return None
    except Exception as es:
        conn.rollback()
        raise Exception(es)
    finally:
        conn.close()


def get_data_source_and_data_type():
    conn = global_pool.connection()
    cursor = conn.cursor()
    try:
        cursor.execute("select distinct data_source, data_type from t_ndc_data_collect_log where data_status=1 and biz_dt > (CURRENT_DATE - 7) order by data_source, data_type")
        result = cursor.fetchall()
        if result:
            return pd.DataFrame(columns=['data_source', 'data_type'], data=result)
        return pd.DataFrame()
    except Exception as es:
        conn.rollback()
        raise Exception(es)
    finally:
        conn.close()


def get_failed_dfcf_collect(biz_dt, data_type):
    conn = global_pool.connection()
    sql =f"""
        select data_text 
          from t_ndc_data_collect_log 
         where data_source = '东方财富' 
           and data_status = 2 
           and log_id = (select max(log_id) 
                           from t_ndc_data_collect_log 
                          where data_source = '东方财富' 
                            and biz_dt = '{biz_dt}' 
                            and data_type = {data_type})
    """
    return sqll.read_sql(sql, conn)