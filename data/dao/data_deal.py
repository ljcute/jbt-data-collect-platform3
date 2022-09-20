from db_connection import global_pool
from utils.logs_utils import logger
from utils.snowflake_utils import get_id


def insert_data_collect(record_num, data_info, date, data_type, data_source, start_dt, end_dt, used_time, data_url,
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
        """, [log_id, date, data_type, data_source, excel_file_path, data_url, record_num, data_info, start_dt, end_dt,
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
