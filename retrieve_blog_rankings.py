import sys
import stat
import os
import re
import traceback
from html.parser import HTMLParser
import psycopg2
import configparser
from time import sleep
import urlnorm
from bs4 import BeautifulSoup
import snac
import requests

AUTH_SCORE_THRESHOLD = 1
TECHNORATI_URL = 'http://technorati.com/blogs/directory/politics/uspolitics/'
# Retrieval of Technorati rankings via archive.org:
#TECHNORATI_URL = (
#'https://web.archive.org/web/20140214212619/http://technorati.com/blogs/directory/politics/uspolitics/'
#)
CRAWL_DELAY = 10
RETRY_ATTEMPTS = 10

rank_regex = re.compile(r"^\s*\d+\.\s*$")
auth_regex = re.compile(r"^\s*Auth:\s*\d+\s*$")

def log(dbinfo, level, message):
    """
    Store a log message in the database.
    """
    sql = ('insert into blog_rank_log ' +
           '(blog_rank_run_id, log_level, message)  ' +
           'values (%s, %s, %s);')
    conn = dbinfo['conn']
    cur = conn.cursor()
    cur.execute(sql, (dbinfo['run_id'], level, message) )
    cur.close()
    conn.commit()

def register_blog(dbinfo, blog):
    """
    Insert blog in database and return blog_id; or if blog already exists,
    return its blog_id.
    """
    sql = 'select blog_id from blog where link = %s'
    conn = dbinfo['conn']
    cur = conn.cursor()
    cur.execute(sql, (blog.get('link'), ) )
    r = cur.fetchone()
    cur.close()
    if r is not None:
        return r[0]    # Return existing blog_id.
    else:
        sql = ('insert into blog (blog_id, link) ' +
               'values (DEFAULT, %s) returning blog_id;')
        cur = conn.cursor()
        cur.execute(sql, (blog.get('link'), ) )
        r = cur.fetchone()
        cur.close()
        return r[0]    # Return new blog_id.

def insert_blog_ranking(dbinfo, blog, blog_id):
    """
    Insert blog ranking in database.
    """
    sql = ('insert into blog_rank ' +
           '(blog_rank_run_id, blog_id, rank, auth_score) ' +
           'values (%s, %s, %s, %s);')
    conn = dbinfo['conn']
    cur = conn.cursor()
    cur.execute(sql,
                (dbinfo['run_id'], blog_id, blog.get('rank'),
                 blog.get('auth_score')) )
    cur.close()

def store_blog_ranking(dbinfo, blog):
    """
    Store extracted blog ranking in database.
    """
    blog_id = register_blog(dbinfo, blog)
    insert_blog_ranking(dbinfo, blog, blog_id)
    dbinfo['conn'].commit()

def parse_site_details(td):
    for a in td.find_all('a'):
        a_class = a.get('class')
        if a_class is not None and 'offsite' in a_class:
            href = a.get('href')
            return href
    return None

def parse_statistics(td):
    for table1 in td.find_all('table'):
        for tr1 in table1.find_all('tr'):
            for td1 in tr1.find_all('td'):
                for strong in td1.find_all('strong'):
                    strong_class = strong.get('class')
                    if (strong_class is not None and
                            'authority-count' in strong_class):
                        for string1 in strong.strings:
                            if (re.match(auth_regex, string1)
                                    is not None):
                                auth = string1.strip()[5:].strip()
                                return auth
    return None

def parse_page(dbinfo, html):
    soup = BeautifulSoup(html, "lxml")
    blog_count = 0
    done = False
    for table in soup.find_all('table'):
        rank = None
        href = None
        auth = None
        for tr in table.find_all('tr'):
            for string in tr.strings:
                if re.match(rank_regex, string) is not None:
                    rank = string.strip()[:-1]
            for td in tr.find_all('td'):
                td_class = td.get('class')
                if td_class is not None and 'site-details' in td_class:
                    site_details = parse_site_details(td)
                    if site_details is not None:
                        href = site_details
                if td_class is not None and 'statistics' in td_class:
                    statistics = parse_statistics(td)
                    if statistics is not None:
                        auth = statistics
        if (rank is not None or href is not None or auth is not None):
            print( (rank, href, auth) )
        if (rank is not None and href is not None and auth is not None):
            if int(auth) > AUTH_SCORE_THRESHOLD:
                blog = {'link': urlnorm.norms(href),
                # Alternative construction for archive.org URLs:
#               blog = {'link': 'https://web.archive.org' + urlnorm.norms(href),
                        'rank': rank,
                        'auth_score': auth}
                store_blog_ranking(dbinfo, blog)
                blog_count += 1
            else:
                done = True
    return (blog_count, done)

def retrieve_page(dbinfo, url):
    """
    Retrieve a web page, with retries if necessary.
    """
    crawl_delay = CRAWL_DELAY
    html = ''
    attempt = 1
    dbinfo['conn'].commit()
    while True:
        try:
            # Retrieve the page.
            headers = {
                'User-Agent':
                ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10.8; rv:21.0) ' +
                 'Gecko/20100101 Firefox/21.0'),
                'Accept':
                ('text/html,application/xhtml+xml,application/xml;' +
                 'q=0.9,*/*;q=0.8') }
            r = requests.get(url, headers=headers, timeout=30)
            html = r.text
            # Extract blog rankings from the retrieved page.
#            parser = TopBlogsHTMLParser(dbinfo)
#            parser.feed(html)
            (blog_count, done) = parse_page(dbinfo, html)
            dbinfo['conn'].commit()
            return (blog_count, done)
        except:
#           traceback.print_exc()
            print("Unexpected error:" + str(sys.exc_info()[0]))
            dbinfo['conn'].rollback()
            if attempt >= RETRY_ATTEMPTS:
                print('Error retrieving web page, too many retries: ' + url)
                log(dbinfo, 'ERROR',
                    'Error retrieving web page, too many retries: ' + url)
                return None
            else:
                print('Problem retrieving web page, retrying: ' + url)
                log(dbinfo, 'WARNING',
                    'Problem retrieving web page, retrying: ' + url)
                sleep(crawl_delay)
                crawl_delay = crawl_delay * 2
                attempt += 1

def retrieve_from_page(dbinfo, page):
    """
    Retrieve a web page containing blog rankings and extract the rankings.
    """
    # Compose URL to retrieve.
    url = TECHNORATI_URL + ('' if page == 1 else 'page-' + str(page) + '/')
    print(url)
    # Retrieve and parse the page, with retries if necessary.
    return retrieve_page(dbinfo, url)

def check_blog_count(total_blog_count, total_pages, blog_count):
    """
    Check that the number of blogs retrieved is the expected number based on
    the number of web pages retrieved, and return True if so.
    """
    expected = 10 + (total_pages - 2) * 25 + blog_count
    return total_blog_count == expected

def init_run(conn):
    """
    Create new run in database, and return run ID (blog_rank_run_id).
    """
    sql = ('insert into blog_rank_run ' +
           '(blog_rank_run_id) ' +
           'values (DEFAULT) returning blog_rank_run_id;')
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
    # Connect to snac database.
    conn = snac.connect_snac_db()
    # Add new run to the database.
    run_id = init_run(conn)
    # Create dbinfo dictionary to hold database access details.
    return {'conn': conn, 'run_id': run_id}

def end_run(dbinfo):
    """
    Store run's ending timestamp in the database.
    """
    sql = ('update blog_rank_run ' +
           'set end_time = CURRENT_TIMESTAMP ' +
           'where blog_rank_run_id = %s;')
    conn = dbinfo['conn']
    cur = conn.cursor()
    cur.execute(sql, (dbinfo['run_id'],) )
    cur.close()
    conn.commit()

def retrieve_ranking_pages(dbinfo):
    """
    Retrieve a series of web pages containing blog rankings, and
    extract the blog details from each page.
    """
    # Retrieve web pages one at a time and store blog rankings in database.
    total_blog_count = 0
    page = 0
    done = False
    while not done:
        page += 1
        if page > 1:
            sleep(CRAWL_DELAY)
        t = retrieve_from_page(dbinfo, page)
        if t is None:
            return -1
        (blog_count, done) = t
        total_blog_count += blog_count
    total_pages = page
    # Check that blogs were not missed by the parser.
    # (Disable this check if running via archive.org.)
    if (check_blog_count(total_blog_count, total_pages, blog_count) == False):
        log(dbinfo, 'ERROR', 'Possibly incorrect number of blogs retrieved: ' +
            str(total_blog_count) + ' blog(s) retrieved from ' +
            str(total_pages) + ' page(s)')
        return -1
    end_run(dbinfo)
    log(dbinfo, 'INFO', 'Run complete')

def retrieve_blog_rankings():
    """
    Retrieve blog rankings and other details from web pages.
    """
    # Initialize the database connection.
    dbinfo = set_up_database()
    # Retrieve all blog rankings.
    try:
        return retrieve_ranking_pages(dbinfo)
    finally:
        dbinfo['conn'].close()

if __name__ == '__main__':
    sys.exit(retrieve_blog_rankings())
