import sys
import urllib.request
from html.parser import HTMLParser
import logging
from bs4 import BeautifulSoup
from time import sleep
import feedparser
import re
import time
import datetime
import requests
import psycopg2
import traceback

import snac
import db

CRAWL_DELAY = 10
RETRY_ATTEMPTS = 3

def log(level, message):
    """
    Store a log message in the database.
    """
    pass
#    sql = ('insert into blog_post_log ' +
#           '(blog_post_run_id, log_level, message)  ' +
#           'values (%s, %s, %s);')
#    conn = snac.connect_snac_db()
#    db.execute( conn, sql, (dbinfo['run_id'], level, message) )
#    conn.commit()

def timestamp():
    ts = time.time()
    st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    return st + '\t'

def get_ranked_blogs(dbinfo, limit = 0):
    """
    Get list of blogs from database in ranked order, optionally up to
    a specified limit.
    """
    conn = snac.connect_snac_db()
    sql_prefix = ('select blog_id, link, rank from blog_rank_latest '
                  'order by rank')
    blogs = ( db.execute_fetchone( conn, sql_prefix + ' limit %s;', (limit, ) )
              if limit > 0 else
              db.execute_fetchone( conn, sql_prefix + ';' ) )
    return blogs

# Move to snac module?
def retrieve_page(url):
    """
    Retrieve a web page, with retries if necessary.
    """
    crawl_delay = CRAWL_DELAY
    html = ''
    attempt = 1
    while True:
        try:
            headers = {
                'User-Agent':
                ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10.8; rv:21.0) ' +
                 'Gecko/20100101 Firefox/21.0'),
                'Accept':
                ('text/html,application/xhtml+xml,application/xml;' +
                 'q=0.9,*/*;q=0.8') }
            r = requests.get(url, headers=headers, timeout=15)
            return r.text
#           request = urllib.request.Request(url, timeout=30)
#           request.add_header(
#               'User-Agent',
#               ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10.8; rv:21.0) ' +
#                'Gecko/20100101 Firefox/21.0') )
#           request.add_header(
#               'Accept',
#               ('text/html,application/xhtml+xml,application/xml;' +
#                'q=0.9,*/*;q=0.8') )
#           html = urllib.request.urlopen(request).read().decode('utf-8')
#           return html
        except:
            if attempt >= RETRY_ATTEMPTS:
                log('ERROR',
                    'Error retrieving web page, too many retries: ' + url)
                return None
            else:
                log('WARNING',
                    'Problem retrieving web page, retrying: ' + url)
                sleep(crawl_delay)
                crawl_delay = crawl_delay * 2
                attempt += 1

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

def retrieve_blog_roll(soup):
    """
    Retrieve blog roll from blog page.
    """
#   print(timestamp() + 'retrieve_blog_roll()')
    blogroll_regex = re.compile(r"BLOGROLL", re.IGNORECASE)
    blog_roll = set()
    for tag in soup.find_all(True):
        tag_id = tag.get('id')
        tag_id = str(tag_id) if tag_id is not None else ''
        tag_class = tag.get('class')
        tag_class = str(tag_class) if tag_class is not None else ''
        if ( re.search(blogroll_regex, tag_id) is not None or
             re.search(blogroll_regex, tag_class) is not None ):
            if tag is not None:
                find_all_a = tag.find_all('a')
                if find_all_a is not None:
                    for a in find_all_a:
                        link = a.get('href')
                        title = a.string if a.string is not None else ''
                        if link is not None and link.startswith('http://'):
#                           print( ( timestamp() + 'Blog roll: ' +
#                                    str((link, title)) ) )
#                            print( (link, title) )
                            blog_roll.add( (link, title) )
    return blog_roll

def parse_rss_urls(soup):
    """
    Parse RSS feed URLs from a web page.
    """
    rss_feeds = []
    for link in soup.find_all('link'):
        if link.get('type') == 'application/rss+xml':
            title = link.get('title')
            href = link.get('href')
            if title is not None and href is not None:
                rss_feeds.append( (title, href) )
    return rss_feeds

def retrieve_rss_feeds(soup):
    """
    Retrieve RSS feeds from blog page.
    """
    try:
        rss_feeds = parse_rss_urls(soup)
    except:
        log('ERROR',
            'Error parsing RSS URLs from web page: ' + url)
        return []
    for r in rss_feeds:
        log('DEBUG',
            'Found RSS feed: ' + r[0] + ' | ' + r[1])
    return rss_feeds

def retrieve_posts(rss_url):
    """
    Parse metadata from an RSS feed.
    """
    return feedparser.parse(rss_url)

def compose_rss_url(blog_url, rss_url):
    """
    Convert an extracted RSS URL, which may be a relative URL, into a full URL.
    """
    if rss_url.startswith('http://') or rss_url.startswith('https://'):
        return rss_url
    else:
        blog_slash = blog_url.endswith('/')
        rss_slash = rss_url.startswith('/')
        if blog_slash or rss_slash:
            if blog_slash and rss_slash:
                return blog_url + rss_url[1:]
            else:
                return blog_url + rss_url
        else:
            return blog_url + '/' + rss_url

def compose_rss_url_archive_org(blog_url, rss_url):
    """
    Convert an extracted RSS URL, which may be a relative URL, into a full URL.
    Similar to compose_rss_url() but intended for blog URLs retrieved via
    archive.org.
    """
    if rss_url.startswith('http://') or rss_url.startswith('https://'):
        return rss_url
    else:
        return 'https://web.archive.org' + rss_url

def parse_blog_post_links(html):
    """
    Parse URL links from a blog post.
    """
    links = []
    soup = BeautifulSoup(html, "lxml")
    for link in soup.find_all('a'):
        href = link.get('href')
        if href is not None:
            links.append(href)
    return links

def exists_blog_post_link(conn, blog_post_id, link):
    sql = ('select blog_post_id '
           'from blog_post_link '
           'where blog_post_id = %s and link = %s;')
    result = db.execute_fetchone( conn, sql, (blog_post_id, link) )
    exists = True if result is not None else False
    return exists

def exists_blog_post(conn, link):
    sql = ('select blog_post_id '
           'from blog_post '
           'where link = %s;')
    result = db.execute_fetchone( conn, sql, (link, ) )
    exists = result is not None
    return exists

def exists_blog_roll_blog_link(conn, blog_roll_id, blog_id):
    sql = ('select blog_roll_id '
           'from blog_roll_blog_link '
           'where blog_roll_id = %s and blog_id = %s;')
    result = db.execute_fetchone( conn, sql, (blog_roll_id, blog_id) )
    exists = result is not None
    return exists

def find_blog_roll(conn, blog_id, link):
    sql = ('select blog_roll_id '
           'from blog_roll '
           'where blog_id = %s and link = %s;')
    r = db.execute_fetchone( conn, sql, (blog_id, link) )
    blog_roll_id = r[0] if r is not None else None
    return blog_roll_id

def store_blog_post_blog_link(conn, blog_post_id, link):
    # Check if link points to post in known blog.
    sql = ('select blog_id '
           'from blog '
           'where link = substring(%s for length(link));')
    blog_id = db.execute_fetchone( conn, sql, (link, ) )
    # Store in blog_post_blog_link
    if blog_id is not None:
        sql = ('insert into blog_post_blog_link '
               '(blog_post_id, link, blog_id) '
               'values (%s, %s, %s);')
        db.execute( conn, sql, (blog_post_id, link, blog_id) )

def store_blog_post_links(conn, blog_post_id, links):
    sql = ('insert into blog_post_link '
           '(blog_post_id, link) '
           'values (%s, %s);')
    for link in links:
#        print('link: ' + link)
        if len(link) > 2000:
            log('ERROR', 'Blog post link too long, ' +
                'blog_post_id = ' + str(blog_post_id) + ': ' + link)
            continue
        if exists_blog_post_link(conn, blog_post_id, link):
            continue
        # Store in blog_post_link
        db.execute( conn, sql, (blog_post_id, link) )
        # Store in blog_post_blog_link
        store_blog_post_blog_link(conn, blog_post_id, link)
        conn.commit()

def store_blog_roll_blog_link(conn, blog_roll_id, link):
    # Check if link points to known blog.
    sql = ('select blog_id '
           'from blog '
           'where link = %s;')
    blog_id = db.execute_fetchone( conn, sql, (link, ) )
    # Store in blog_roll_blog_link
    if blog_id is not None:
        if ( exists_blog_roll_blog_link(conn, blog_roll_id, blog_id) ==
                False ):
            sql = ('insert into blog_roll_blog_link '
                   '(blog_roll_id, blog_id) '
                   'values (%s, %s);')
            db.execute( conn, sql, (blog_roll_id, blog_id) )

def store_blog_roll(run_id, conn, blog_id, blog_roll):
    """
    Store blog roll in database.
    """
    sql = ('insert into blog_roll '
           '(blog_roll_id, blog_post_run_id, blog_id,'
           ' link, title) '
           'values (DEFAULT, %s, %s, %s, %s) '
           'returning blog_roll_id;')
    for t in blog_roll:
        (link, title) = t
        blog_roll_id = find_blog_roll(conn, blog_id, link)
        if blog_roll_id is None:
            r = db.execute_fetchone( conn, sql, (run_id, blog_id, link, title) )
            blog_roll_id = r[0]
        store_blog_roll_blog_link(conn, blog_roll_id, link)
    conn.commit()

def store_blog_posts(run_id, conn, blog_id, rss_entries):
    """
    Store blog posts from RSS in the database.
    """
#   print(timestamp() + 'store_blog_posts()')
    sql = ('insert into blog_post '
           '(blog_post_id, blog_post_run_id, blog_id,'
           ' author, content, link, published, title) '
           'values (DEFAULT, %s, %s, %s, %s, %s, %s, %s) '
           'returning blog_post_id;')
    for entry in rss_entries:
        link = entry.get('link', '')
#       print(timestamp() + 'Blog post: ' + link)
        if link is None or link == '' or exists_blog_post(conn, link):
            continue
#        print('post: ' + entry.get('link', ''))
#        import json
#        print(json.dumps(entry, sort_keys = True, indent = 4))
        author = entry.get('author', '').strip()
        content = entry.get('content', [{}])[0].get('value', '').strip()
        published = entry.get('published', '').strip()
        title = entry.get('title', '').strip()
        blog_post_id = None
        try:
            r = db.execute_fetchone( conn, sql, (run_id, blog_id,
                                     author,
                                     content,
                                     link,
                                     published if published != '' else None,
                                     title) )
            blog_post_id = r[0]
        except Exception:
            pass
        if blog_post_id is not None and content is not None:
            links = parse_blog_post_links(content)
            store_blog_post_links(conn, blog_post_id, links)
    conn.commit()

def retrieve_site(run_id, blog_id, blog_link, blog_rank):
    print('CONNECTING ' + str((blog_id, blog_link, blog_rank)) )
    conn = snac.connect_snac_db()
    print('CONNECTED ' + str((blog_id, blog_link, blog_rank)) )
    try:
        print(timestamp() + 'Blog: ' + str((blog_id, blog_link, blog_rank)) )
        print('00')
        html = retrieve_page(blog_link)
        print('01')
        if html is None:
            log('DEBUG', 'Page not retrieved')
            return
        print('02')
        soup = BeautifulSoup(html, "lxml")
        # Get blog roll
        print('03')
        blog_roll = retrieve_blog_roll(soup)
        print('04')
        store_blog_roll(run_id, conn, blog_id, blog_roll)
        # Get RSS feeds
        print('05')
        rss_feeds = retrieve_rss_feeds(soup)
        print('06')
        if len(rss_feeds) == 0:
            log('DEBUG', 'No RSS feeds found')
            return
        print('07')
        main_rss_url = rss_feeds[0][1]
        print('08')
        composed_rss_url = compose_rss_url(blog_link, main_rss_url)
        print('09')
        log('DEBUG', 'Retrieving posts: ' + composed_rss_url)
        print('10')
        rss = retrieve_posts(composed_rss_url)
        print('11')
        rss_entries = rss['entries']
        print(blog_link + ' - ' + str(len(rss_entries)) + ' rss entries')
        print('12')
        store_blog_posts(run_id, conn, blog_id, rss_entries)
    except:
        print(traceback.format_exc())
    finally:
        conn.close()

if __name__ == '__main__':
    run_id = sys.argv[1]
    blog_id = sys.argv[2]
    blog_link = sys.argv[3]
    blog_rank = sys.argv[4]
    sys.exit( retrieve_site(run_id, blog_id, blog_link, blog_rank) )

