import sys
import stat
import os
import psycopg2
import configparser

def connect_db(dbname, user, password):
    cn = psycopg2.connect('dbname=' + dbname +
                          ' user=' + user +
                          ' password=' + password)
    # Setting the transaction isolation level appears to block in concurrent
    # connections in retrieve_blog_posts_parallel.py.  For this reason it is
    # commented out.
#    cn.set_session(isolation_level = 'SERIALIZABLE')
#    cn.set_session(isolation_level = 'READ COMMITTED')
    return cn

def accessible_by_group_or_world(file):
    st = os.stat(file)
    return bool( st.st_mode & (stat.S_IRWXG | stat.S_IRWXO) )

def get_config():
    config_file = os.environ['HOME'] + '/.snac'
    if accessible_by_group_or_world(config_file):
        print ('ERROR: config file ' + config_file + ' has group or world ' +
        'access; permissions should be set to u=rw')
        sys.exit(1)
    config = configparser.RawConfigParser()
    config.read(config_file)
    return config

def connect_snac_db():
    config = get_config()
    conn = connect_db(config.get('default', 'dbname'),
                      config.get('default', 'user'),
                      config.get('default', 'password'))
    return conn

if __name__ == '__main__':
    print('This is a function library for snac tools.')
