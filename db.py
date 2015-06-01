import os
import psycopg2
import psycopg2.extras
import snac

def connect():
    return snac.connect_snac_db()

def cursor(conn):
    return conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)

def print_with_pid(message):
    pid = str(os.getpid())
    sp = message.split('\n')
    for s in sp:
        print('[{:s}] {:s}'.format(pid, s))

def print_exception(e):
    if e.pgerror is not None:
        print_with_pid(str(e.pgerror))
    if e.diag.message_detail is not None:
        print_with_pid(str(e.diag.message_detail))

def execute(conn, sql):
    with cursor(conn) as cur:
        try:
            cur.execute(sql)
        except psycopg2.Error as e:
            print_exception(e)

def execute(conn, sql, data):
    with cursor(conn) as cur:
        try:
            cur.execute(sql, data)
        except psycopg2.Error as e:
            print_exception(e)

def execute_fetchone(conn, sql):
    with cursor(conn) as cur:
        try:
            cur.execute(sql)
            return cur.fetchone()
        except psycopg2.Error as e:
            print_exception(e)

def execute_fetchone(conn, sql, data):
    with cursor(conn) as cur:
        try:
            cur.execute(sql, data)
            return cur.fetchone()
        except psycopg2.Error as e:
            print_exception(e)

if __name__ == '__main__':
    pass

