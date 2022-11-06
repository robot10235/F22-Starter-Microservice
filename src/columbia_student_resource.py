import pymysql

import os


class ColumbiaStudentResource:

    def __int__(self):
        pass

    @staticmethod
    def _get_connection():
        usr = os.environ.get("DBUSER")
        pw = os.environ.get("DBPW")
        h = os.environ.get("DBHOST")

        conn = pymysql.connect(
            user=usr,
            password=pw,
            host=h,
            # user="root",
            # password="123456",
            # host="localhost",
            port=3306,
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True
        )
        return conn

    @staticmethod
    def get_by_key(key):
        sql = "SELECT * FROM f22_databases.columbia_students where guid=%s;"
        conn = ColumbiaStudentResource._get_connection()
        cur = conn.cursor()
        res = cur.execute(sql, args=key)
        result = cur.fetchone()
        conn.commit()
        conn.close()
        return result

    @staticmethod
    def get_all():
        sql = "SELECT * FROM f22_databases.columbia_students;"
        conn = ColumbiaStudentResource._get_connection()
        cur = conn.cursor()
        res = cur.execute(sql)
        result = cur.fetchall()
        conn.commit()
        conn.close()
        return result

    @staticmethod
    def get_page(page_num):
        offset = (int(page_num)-1)*10
        sql = "SELECT * FROM f22_databases.columbia_students LIMIT 10 OFFSET %s;"
        conn = ColumbiaStudentResource._get_connection()
        cur = conn.cursor()
        res = cur.execute(sql, args=offset)
        result = cur.fetchall()
        conn.commit()
        conn.close()
        return result

    @staticmethod
    def add_one(data):
        guid = data.get('guid', '')
        last_name = data.get('last_name', '')
        first_name = data.get('first_name', '')
        middle_name = data.get('middle_name', '')
        email = data.get('email', '')
        school_code = data.get('school_code', '')
        sql = f'INSERT INTO f22_databases.columbia_students \
        (guid, last_name, first_name, middle_name, email, school_code) \
        VALUES (\'{guid}\', \'{last_name}\', \'{first_name}\', \'{middle_name}\', \'{email}\', \'{school_code}\');'
        conn = ColumbiaStudentResource._get_connection()
        cur = conn.cursor()
        res = cur.execute(sql)
        conn.commit()
        conn.close()
        return res

    @staticmethod
    def delete_by_key(key):
        sql = "DELETE FROM f22_databases.columbia_students WHERE guid=%s;"
        conn = ColumbiaStudentResource._get_connection()
        cur = conn.cursor()
        res = cur.execute(sql, args=key)
        conn.commit()
        conn.close()
        return res

    @staticmethod
    def update_by_key(key, data):
        last_name = data.get('last_name', '')
        first_name = data.get('first_name', '')
        middle_name = data.get('middle_name', '')
        email = data.get('email', '')
        school_code = data.get('school_code', '')
        sql = f'UPDATE f22_databases.columbia_students  \
              SET last_name = \'{last_name}\', first_name = \'{first_name}\', middle_name = \'{middle_name}\', email = \'{email}\', school_code = \'{school_code}\' \
              WHERE guid=%s;'
        conn = ColumbiaStudentResource._get_connection()
        cur = conn.cursor()
        res = cur.execute(sql, args=key)
        conn.commit()
        conn.close()
        return res
