import sys
import urllib.request
from html.parser import HTMLParser
import logging
from bs4 import BeautifulSoup
from time import sleep
import feedparser
import snac
import re
import time
import datetime

#MAXIMUM_BLOG_COUNT = 100    # Set to 0 for no limit.
MAXIMUM_BLOG_COUNT = 0
CRAWL_DELAY = 10
RETRY_ATTEMPTS = 10

def log(dbinfo, level, message):
    """
    Store a log message in the database.
    """
    sql = ('insert into blog_post_log ' +
           '(blog_post_run_id, log_level, message)  ' +
           'values (%s, %s, %s);')
    conn = dbinfo['conn']
    cur = conn.cursor()
    cur.execute(sql, (dbinfo['run_id'], level, message) )
    cur.close()
    conn.commit()

def timestamp():
    ts = time.time()
    st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    return st + '\t'

def get_ranked_blogs(dbinfo, limit = 0):
    """
    Get list of blogs from database in ranked order, optionally up to
    a specified limit.
    """
    conn = dbinfo['conn']
    cur = conn.cursor()
    sql_prefix = ('select blog_id, link, rank from blog_rank_latest '
                  'order by rank')
    if limit > 0:
        cur.execute(sql_prefix + ' limit %s;', (limit, ))
    else:
        cur.execute(sql_prefix + ';')
    blogs = cur.fetchall()
    cur.close()
    return blogs

# Move to snac module?
def retrieve_page(dbinfo, url):
    """
    Retrieve a web page, with retries if necessary.
    """
    crawl_delay = CRAWL_DELAY
    html = ''
    attempt = 1
    while True:
        try:
            request = urllib.request.Request(url)
            request.add_header(
                'User-Agent',
                ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10.8; rv:21.0) ' +
                 'Gecko/20100101 Firefox/21.0') )
            request.add_header(
                'Accept',
                ('text/html,application/xhtml+xml,application/xml;' +
                 'q=0.9,*/*;q=0.8') )
            html = urllib.request.urlopen(request).read().decode('utf-8')
            return html
        except:
            if attempt >= RETRY_ATTEMPTS:
                log(dbinfo, 'ERROR',
                    'Error retrieving web page, too many retries: ' + url)
                return None
            else:
                log(dbinfo, 'WARNING',
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

def retrieve_blog_roll(dbinfo, soup):
    """
    Retrieve blog roll from blog page.
    """
    print(timestamp() + 'retrieve_blog_roll()')
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
                            print( ( timestamp() + 'Blog roll: ' +
                                     str((link, title)) ) )
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

def retrieve_rss_feeds(dbinfo, soup):
    """
    Retrieve RSS feeds from blog page.
    """
    try:
        rss_feeds = parse_rss_urls(soup)
    except:
        log(dbinfo, 'ERROR',
            'Error parsing RSS URLs from web page: ' + url)
        return []
    for r in rss_feeds:
        log(dbinfo, 'DEBUG',
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
    if rss_url.startswith('http://'):
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

def end_run(dbinfo):
    """
    Store run's ending timestamp in the database.
    """
    sql = ('update blog_post_run ' +
           'set end_time = CURRENT_TIMESTAMP ' +
           'where blog_post_run_id = %s;')
    conn = dbinfo['conn']
    cur = conn.cursor()
    cur.execute(sql, (dbinfo['run_id'],) )
    cur.close()
    conn.commit()

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

def exists_blog_post_link(dbinfo, blog_post_id, link):
    sql = ('select blog_post_id '
           'from blog_post_link '
           'where blog_post_id = %s and link = %s;')
    conn = dbinfo['conn']
    cur = conn.cursor()
    cur.execute(sql, (blog_post_id, link) )
    exists = True if cur.fetchone() is not None else False
    cur.close()
    return exists

def exists_blog_post(dbinfo, link):
    sql = ('select blog_post_id '
           'from blog_post '
           'where link = %s;')
    conn = dbinfo['conn']
    cur = conn.cursor()
    cur.execute(sql, (link, ) )
    exists = True if cur.fetchone() is not None else False
    cur.close()
    return exists

def exists_blog_roll_blog_link(dbinfo, blog_roll_id, blog_id):
    sql = ('select blog_roll_id '
           'from blog_roll_blog_link '
           'where blog_roll_id = %s and blog_id = %s;')
    conn = dbinfo['conn']
    cur = conn.cursor()
    cur.execute(sql, (blog_roll_id, blog_id) )
    exists = True if cur.fetchone() is not None else False
    cur.close()
    return exists

def find_blog_roll(dbinfo, blog_id, link):
    sql = ('select blog_roll_id '
           'from blog_roll '
           'where blog_id = %s and link = %s;')
    conn = dbinfo['conn']
    cur = conn.cursor()
    cur.execute(sql, (blog_id, link) )
    r = cur.fetchone()
    blog_roll_id = r[0] if r is not None else None
    cur.close()
    return blog_roll_id

def store_blog_post_blog_link(dbinfo, blog_post_id, link):
    # Check if link points to post in known blog.
    sql = ('select blog_id '
           'from blog '
           'where link = substring(%s for length(link));')
    conn = dbinfo['conn']
    cur = conn.cursor()
    cur.execute(sql, (link, ) )
    blog_id = cur.fetchone()
    cur.close()
    # Store in blog_post_blog_link
    if blog_id is not None:
        sql = ('insert into blog_post_blog_link '
               '(blog_post_id, link, blog_id) '
               'values (%s, %s, %s);')
        conn = dbinfo['conn']
        cur = conn.cursor()
        cur.execute(sql, (blog_post_id, link, blog_id) )
        cur.close()

def store_blog_post_links(dbinfo, blog_post_id, links):
    sql = ('insert into blog_post_link '
           '(blog_post_id, link) '
           'values (%s, %s);')
    conn = dbinfo['conn']
    for link in links:
#        print('link: ' + link)
        if len(link) > 2000:
            log(dbinfo, 'ERROR', 'Blog post link too long, ' +
                'blog_post_id = ' + str(blog_post_id) + ': ' + link)
            continue
        if exists_blog_post_link(dbinfo, blog_post_id, link):
            continue
        # Store in blog_post_link
        cur = conn.cursor()
        cur.execute(sql, (blog_post_id, link) )
        cur.close()
        # Store in blog_post_blog_link
        store_blog_post_blog_link(dbinfo, blog_post_id, link)
    conn.commit()

def store_blog_roll_blog_link(dbinfo, blog_roll_id, link):
    # Check if link points to known blog.
    sql = ('select blog_id '
           'from blog '
           'where link = %s;')
    conn = dbinfo['conn']
    cur = conn.cursor()
    cur.execute(sql, (link, ) )
    blog_id = cur.fetchone()
    cur.close()
    # Store in blog_roll_blog_link
    if blog_id is not None:
        if exists_blog_roll_blog_link(dbinfo, blog_roll_id, blog_id) == False:
            sql = ('insert into blog_roll_blog_link '
                   '(blog_roll_id, blog_id) '
                   'values (%s, %s);')
            conn = dbinfo['conn']
            cur = conn.cursor()
            cur.execute(sql, (blog_roll_id, blog_id) )
            cur.close()

def store_blog_roll(dbinfo, blog_id, blog_roll):
    """
    Store blog roll in database.
    """
    sql = ('insert into blog_roll '
           '(blog_roll_id, blog_post_run_id, blog_id,'
           ' link, title) '
           'values (DEFAULT, %s, %s, %s, %s) '
           'returning blog_roll_id;')
    conn = dbinfo['conn']
    for t in blog_roll:
        (link, title) = t
        blog_roll_id = find_blog_roll(dbinfo, blog_id, link)
        if blog_roll_id is None:
            cur = conn.cursor()
            cur.execute(sql, (dbinfo['run_id'], blog_id,
                              link, title) )
            r = cur.fetchone()
            cur.close()
            blog_roll_id = r[0]
        store_blog_roll_blog_link(dbinfo, blog_roll_id, link)
    conn.commit()

def store_blog_posts(dbinfo, blog_id, rss_entries):
    """
    Store blog posts from RSS in the database.
    """
    print(timestamp() + 'store_blog_posts()')
    sql = ('insert into blog_post '
           '(blog_post_id, blog_post_run_id, blog_id,'
           ' author, content, link, published, title) '
           'values (DEFAULT, %s, %s, %s, %s, %s, %s, %s) '
           'returning blog_post_id;')
    conn = dbinfo['conn']
    for entry in rss_entries:
        link = entry.get('link', None)
        print(timestamp() + 'Blog post: ' + (link if link is not None else '') )
        if exists_blog_post(dbinfo, link):
            continue
#        print('post: ' + entry.get('link', ''))
#        import json
#        print(json.dumps(entry, sort_keys = True, indent = 4))
        author = entry.get('author', '').strip()
        content = entry.get('content', [{}])[0].get('value', '').strip()
        published = entry.get('published', '').strip()
        title = entry.get('title', '').strip()
        cur = conn.cursor()
        cur.execute(sql, (dbinfo['run_id'], blog_id,
                          author,
                          content,
                          link,
                          published if published != '' else None,
                          title) )
        r = cur.fetchone()
        cur.close()
        blog_post_id = r[0]
        if content is not None:
            links = parse_blog_post_links(content)
            store_blog_post_links(dbinfo, blog_post_id, links)
    conn.commit()

def retrieve_blog_site_data(dbinfo):
    """
    Retrieve RSS feeds and blog postings from ranked blogs.
    """
    blogs = get_ranked_blogs(dbinfo, MAXIMUM_BLOG_COUNT)
    for blog in blogs:
        print(timestamp() + 'Blog: ' + str(blog))
        (blog_id, blog_link, blog_rank) = blog
#        if blog_link != '...a_problem_url...':
#            continue
        html = retrieve_page(dbinfo, blog_link)
        if html is None:
            log(dbinfo, 'DEBUG', 'Page not retrieved')
            continue
        soup = BeautifulSoup(html, "lxml")
#        print('**************************************************************')
#        print('* ' + blog_link)
#        print('**************************************************************')
        # Get blog roll
        blog_roll = retrieve_blog_roll(dbinfo, soup)
        store_blog_roll(dbinfo, blog_id, blog_roll)
        # Get RSS feeds
        rss_feeds = retrieve_rss_feeds(dbinfo, soup)
        if len(rss_feeds) == 0:
            log(dbinfo, 'DEBUG', 'No RSS feeds found')
            continue
        main_rss_url = rss_feeds[0][1]
        composed_rss_url = compose_rss_url(blog_link, main_rss_url)
        log(dbinfo, 'DEBUG', 'Retrieving posts: ' + composed_rss_url)
        rss = retrieve_posts(composed_rss_url)
        rss_entries = rss['entries']
        store_blog_posts(dbinfo, blog_id, rss_entries)
    end_run(dbinfo)
    log(dbinfo, 'INFO', 'Run complete')

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
    # Connect to snac database.
    conn = snac.connect_snac_db()
    # Add new run to the database.
    run_id = init_run(conn)
    # Create dbinfo dictionary to hold database access details.
    return {'conn': conn, 'run_id': run_id}

def retrieve_blog_posts():
    """
    Retrieve posts from ranked blogs.
    """
    # Initialize the database connection.
    dbinfo = set_up_database()
    # Retrieve all blog rankings.
    try:
        return retrieve_blog_site_data(dbinfo)
    finally:
        dbinfo['conn'].close()

if __name__ == '__main__':
    sys.exit(retrieve_blog_posts())

