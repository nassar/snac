import sys
import urllib.request
from html.parser import HTMLParser
import logging
import subprocess
from bs4 import BeautifulSoup
from time import sleep
import feedparser
import snac
import re
import time
import datetime
from threading import Thread

#MAXIMUM_BLOG_COUNT = 100    # Set to 0 for no limit.
MAXIMUM_BLOG_COUNT = 0

def log(level, message):
    """
    Store a log message in the database.
    """
    return
#   sql = ('insert into blog_post_log ' +
#          '(blog_post_run_id, log_level, message)  ' +
#          'values (%s, %s, %s);')
#   conn = snac.connect_snac_db()
#   try:
#       cur = conn.cursor()
#       cur.execute(sql, (dbinfo['run_id'], level, message) )
#       cur.close()
#       conn.commit()
#   finally:
#       conn.close()

def get_ranked_blogs(limit = 0):
    """
    Get list of blogs from database in ranked order, optionally up to
    a specified limit.
    """
    conn = snac.connect_snac_db()
    try:
        cur = conn.cursor()
        sql_prefix = ('select blog_id, link, rank from blog_rank_latest '
                      'order by rank')
        if limit > 0:
            cur.execute(sql_prefix + ' limit %s;', (limit, ))
        else:
            cur.execute(sql_prefix + ';')
        blogs = cur.fetchall()
        cur.close()
    finally:
        conn.close()
    return blogs

#class RSSHTMLParser(HTMLParser):
#    def __init__(self):
#        HTMLParser.__init__(self)
#        self.rss_urls = []
#    def contains_attr(self, attrs, name, value):
#        try:
#            x = attrs.index( (name, value) )
#            return True
#        except ValueError:
#            return False
#    def handle_starttag(self, tag, attrs):
#        if (tag == 'link' and
#                self.contains_attr(attrs, 'type', 'application/rss+xml')):
#            title = list(filter(lambda t : t[0] == 'title', attrs))[0][1]
#            href = list(filter(lambda t : t[0] == 'href', attrs))[0][1]
#            self.rss_urls.append((title, href))
#
#def retrieve_rss_urls(html):
#    parser = RSSHTMLParser()
#    parser.feed(html)
#    return parser.rss_urls

def end_run(run_id):
    """
    Store run's ending timestamp in the database.
    """
    sql = ('update blog_post_run ' +
           'set end_time = CURRENT_TIMESTAMP ' +
           'where blog_post_run_id = %s;')
    conn = snac.connect_snac_db()
    try:
        cur = conn.cursor()
        cur.execute(sql, (run_id,) )
        cur.close()
        conn.commit()
    finally:
        conn.close()

def exists_blog_post_link(dbinfo, conn, blog_post_id, link):
    sql = ('select blog_post_id '
           'from blog_post_link '
           'where blog_post_id = %s and link = %s;')
    cur = conn.cursor()
    cur.execute(sql, (blog_post_id, link) )
    exists = True if cur.fetchone() is not None else False
    cur.close()
    return exists

def exists_blog_roll_blog_link(dbinfo, conn, blog_roll_id, blog_id):
    sql = ('select blog_roll_id '
           'from blog_roll_blog_link '
           'where blog_roll_id = %s and blog_id = %s;')
    cur = conn.cursor()
    cur.execute(sql, (blog_roll_id, blog_id) )
    exists = True if cur.fetchone() is not None else False
    cur.close()
    return exists

def store_blog_post_blog_link(dbinfo, conn, blog_post_id, link):
    # Check if link points to post in known blog.
    sql = ('select blog_id '
           'from blog '
           'where link = substring(%s for length(link));')
    cur = conn.cursor()
    cur.execute(sql, (link, ) )
    blog_id = cur.fetchone()
    cur.close()
    # Store in blog_post_blog_link
    if blog_id is not None:
        sql = ('insert into blog_post_blog_link '
               '(blog_post_id, link, blog_id) '
               'values (%s, %s, %s);')
        cur = conn.cursor()
        cur.execute(sql, (blog_post_id, link, blog_id) )
        cur.close()

class RetrieveSiteThread(Thread):
    def __init__(self, run_id, blog_id, blog_link, blog_rank):
        Thread.__init__(self)
        self.run_id = run_id
        self.blog_id = blog_id
        self.blog_link = blog_link
        self.blog_rank = blog_rank
    def run(self):
        proc = [ 'python3',
                 'retrieve_blog_posts_subproc.py',
                 str(self.run_id),
                 str(self.blog_id),
                 self.blog_link,
                 str(self.blog_rank) ]
        subprocess.call(proc)

def retrieve_blog_site_data(run_id):
    """
    Retrieve RSS feeds and blog postings from ranked blogs.
    """
    blogs = get_ranked_blogs(MAXIMUM_BLOG_COUNT)
    threads = []
    max_threads = 45    # Number of simultaneous threads
    for blog in blogs:
        # Wait until the number of simultaneous threads falls below max_threads
        while len(threads) >= max_threads:
            sleep(10)
            print('threads (old) ' + str(len(threads)))
            threads = [th for th in threads if th.is_alive() == True]
            print('threads (new) ' + str(len(threads)))
        print('exit while loop')
        # Start a new thread
        (blog_id, blog_link, blog_rank) = blog
        th = RetrieveSiteThread(run_id, blog_id, blog_link, blog_rank)
        th.setName(str(blog))
        th.start()
        threads.append(th)
    print('exit for loop')
    # Join remaining threads
    for th in threads:
        print('join')
        th.join()
    print('exit join loop')
    end_run(run_id)
    log('INFO', 'Run complete')

def init_run(conn):
    """
    Create new run in database, and return run ID (blog_post_run_id).
    """
    sql = ('insert into blog_post_run ' +
           '(blog_post_run_id) ' +
           'values (DEFAULT) returning blog_post_run_id;')
    cur = conn.cursor()
    cur.execute(sql)
    r = cur.fetchone()
    cur.close()
    conn.commit()
    return r[0]    # run ID

def set_up_database():
    """
    Set up access to the database for a new run.
    """
    conn = snac.connect_snac_db()
    run_id = None
    try:
        run_id = init_run(conn)
    finally:
        conn.close()
    return run_id

def retrieve_blog_posts():
    """
    Retrieve posts from ranked blogs.
    """
    run_id = set_up_database()
    return retrieve_blog_site_data(run_id)

if __name__ == '__main__':
    sys.exit(retrieve_blog_posts())

