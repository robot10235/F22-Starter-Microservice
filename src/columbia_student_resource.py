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
    def update_by_key(id, data):
        column_name = []
        result_list = []
        set_length = len(data)
        if set_length == 0:
            set_str = ""
        else:
            set_str = ", ".join(["{} = %s"] * set_length)
            for i in data.items():
                column_name.append(str(i[0]))
                result_list.append(str(i[1]))  # type  attention!

        # where
        where_str = " where guid = %s"
        result_list.append(id)

        arguments = tuple(result_list)
        sql_row = "UPDATE f22_databases.columbia_students" + " set " + set_str + where_str
        sql = sql_row.format(*column_name)
        conn = ColumbiaStudentResource._get_connection()
        cursor = conn.cursor()
        res = cursor.execute(sql, arguments)

        if res == 1:
            result = cursor.fetchone()
            cursor.close()
            return 1
        else:
            cursor.close()
            return 0
