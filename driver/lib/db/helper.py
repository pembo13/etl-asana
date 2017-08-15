import sqlite3
import json

CONN = None

def get_conn():
    global CONN
    if not CONN:
        CONN = sqlite3.connect('.butter.db')
        CONN.row_factory = dict_factory

    return CONN

def get_cursor():
    return get_conn().cursor()

def initialize_tables():
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute('SELECT name FROM sqlite_master WHERE type="table"')
    tables = cursor.fetchall()

    if not tables:
        sql = """
            CREATE TABLE datasource (
                butter_user_id int,
                datasource_user_id text,
                milestone text,
                UNIQUE (butter_user_id, datasource_user_id)
            )
        """
        cursor.execute(sql)

def get_milestone(butter_user_id, datasource_user_id):
    cursor = get_cursor()

    sql = """
        SELECT milestone FROM datasource WHERE butter_user_id = ? AND
                                               datasource_user_id = ?
    """
    cursor.execute(sql, (butter_user_id, datasource_user_id))
    result = cursor.fetchone()
    if result:
        return json.loads(result['milestone'])

    return None

def upsert_milestone(butter_user_id, datasource_user_id, milestone):
    cursor = get_cursor()
    if type(milestone) != dict:
        raise ValueError('milestone must be of type `dict`')

    sql = """
        INSERT OR REPLACE INTO datasource
            (butter_user_id, datasource_user_id, milestone)
        VALUES
            (?, ?, ?)
    """
    cursor.execute(sql, (butter_user_id, datasource_user_id, json.dumps(milestone)))
    get_conn().commit()


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d
