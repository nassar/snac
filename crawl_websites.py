from datetime import datetime
import re
import os
import subprocess
import shutil
from threading import Thread
from sqlalchemy import create_engine, select, func
from sqlalchemy import Table, Column, MetaData
from sqlalchemy import Sequence, ForeignKey
from sqlalchemy import String, BigInteger, DateTime
from sqlalchemy.sql import and_, or_, not_
from bs4 import BeautifulSoup

base_dir = '/var/lib/snac'
crawl_dir = base_dir + '/crawl'
crawl_data_dir = crawl_dir + '/data'
crawl_tmp_dir = base_dir + '/crawl_tmp'
crawl_tmp_data_dir = crawl_tmp_dir + '/data'

date_regex = re.compile(r"^\d\d\d\d-\d\d-\d\d$")
time_regex = re.compile(r"^\d\d:\d\d:\d\d$")

def mkdir(dir_name):
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)

def rm_recursive(dir_name):
    if os.path.exists(dir_name):
        shutil.rmtree(dir_name)

def make_directories():
    mkdir(base_dir)
    mkdir(crawl_dir)
    mkdir(crawl_tmp_dir)

def git(args):
    proc = ['git',
            '--git-dir=' + crawl_dir + '/.git',
            '--work-tree=' + crawl_dir]
    subprocess.call(proc + args)

def git_init():
    git(['init'])

class SnacDatabase():
    def __init__(self):
        self.meta = MetaData()
        self.website = Table( 'website', self.meta,
            Column('website_id', BigInteger, primary_key=True),
            Column('link', String, nullable=False, unique=True) )
        self.page = Table( 'page', self.meta,
            Column('page_id', BigInteger, primary_key=True),
            Column('link', String, nullable=False, unique=True),
            Column('website_id', None, ForeignKey('website.website_id')) )
        self.website_run = Table( 'website_run', self.meta,
            Column('website_run_id', BigInteger, primary_key=True),
            Column('start_time', DateTime(timezone=True), nullable=False,
                   default=datetime.now()),
            Column('end_time', DateTime(timezone=True)) )
        self.page_data = Table( 'page_data', self.meta,
            Column('website_run_id', None,
                   ForeignKey('website_run.website_run_id'), primary_key=True),
            Column('page_id', None, ForeignKey('page.page_id'),
                   primary_key=True),
            Column('title', String, nullable=False),
            Column('full_text', String, nullable=False) )
        self.page_link = Table( 'page_link', self.meta,
            Column('website_run_id', None,
                   ForeignKey('website_run.website_run_id'), primary_key=True),
            Column('page_id', None, ForeignKey('page.page_id'),
                   primary_key=True),
            Column('link', String, nullable=False, primary_key=True),
            Column('link_text', String, nullable=False) )
        self.engine = create_engine(
            'postgresql://snac_admin:fjv8tvbe4EwD311@localhost:5432/snac',
            echo=True )
        self.conn = self.engine.connect()
        self.trans = self.conn.begin()
    def create_schema(self):
        self.meta.create_all(self.engine)
    def insert_website(self, website_url):
        ins = self.website.insert().values(link=website_url)
        result = self.conn.execute(ins)
    def select_website(self):
        s = select([self.website])
        result = self.conn.execute(s)
        r = result.fetchall()
        result.close()
        return r
    def insert_page(self, page_url, website_id):
        ins = self.page.insert().values(link=page_url, website_id=website_id)
        result = self.conn.execute(ins)
        page_id = result.inserted_primary_key[0]
        return page_id
    def select_page(self, page_url):
        s = select([self.page.c.page_id]).where(self.page.c.link == page_url)
        result = self.conn.execute(s)
        row = result.fetchone()
        r = row['page_id'] if row is not None else None
        result.close()
        return r
    def register_page(self, page_url, website_id):
        page_id = self.select_page(page_url)
        if page_id is None:
            ins = self.page.insert().values(link=page_url,
                                            website_id=website_id)
            result = self.conn.execute(ins)
            page_id = result.inserted_primary_key[0]
            return page_id
        else:
            return page_id
    def insert_website_run(self):
        ins = self.website_run.insert().values()
        result = self.conn.execute(ins)
        website_run_id = result.inserted_primary_key[0]
        return website_run_id
    def end_website_run(self, website_run_id):
        upd = ( self.website_run.update().
                    where(
                        self.website_run.c.website_run_id == website_run_id ).
                    values(
                        end_time=func.current_timestamp() )
              )
        result = self.conn.execute(upd)
    def insert_page_data(self, website_run_id, page_id, title, full_text):
        ins = self.page_data.insert().values(
            website_run_id=website_run_id,
            page_id=page_id,
            title=(title if title is not None else ''),
            full_text=(full_text if full_text is not None else ''))
        result = self.conn.execute(ins)
    def insert_page_link(self, website_run_id, page_id, link, link_text):
        ins = self.page_link.insert().values(
            website_run_id=website_run_id,
            page_id=page_id,
            link=(link[:4096] if link is not None else ''),
            link_text=(link_text if link_text is not None else ''))
        result = self.conn.execute(ins)
    def select_page_link(self, website_run_id, page_id, link):
        s = select([self.page_link]).where( and_(
            self.page_link.c.website_run_id == website_run_id,
            self.page_link.c.page_id == page_id,
            self.page_link.c.link == link ) )
        result = self.conn.execute(s)
        r = result.fetchone()
        result.close()
        return r
    def commit(self):
        self.trans.commit()

def import_website_urls(db, file_name):
    with open(file_name) as f:
        for line in f:
            db.insert_website(line.strip())

class WgetThread(Thread):
    def __init__(self, url, output_dir):
        Thread.__init__(self)
        self.url = url
        self.output_dir = output_dir
    def run(self):
        proc = ['wget',
                '-nv',
                '--output-file=' + self.output_dir + '.log',
                '-r',
                '-np',
                '-A',
                'html,htm,xhtml,php,jsp,asp,cfm',
                '--directory-prefix=' + self.output_dir,
                '--protocol-directories',
                '--tries=10',
                '--timeout=30',
                '--wait=20',
                '--random-wait',
                '--no-cookies',
                self.url]
        subprocess.call(proc)

def crawl(db):
    rm_recursive(crawl_tmp_data_dir)
    mkdir(crawl_tmp_data_dir)
    websites = db.select_website()
    threads = []
    for w in websites:
        website_id = str(w['website_id'])
        url = w['link']
        print(str(website_id) + '\t' + url)
        output_dir = crawl_tmp_data_dir + '/' + website_id
        th = WgetThread(url, output_dir)
        th.setName(url)
        th.start()
        threads.append(th)
    for th in threads:
        th.join()

def git_snapshot():
    rm_recursive(crawl_data_dir)
    shutil.copytree(crawl_tmp_data_dir, crawl_data_dir)
    git(['add', crawl_data_dir])
    git(['add', '-u', crawl_data_dir])
    git(['commit', '-m', 'Crawl snapshot'])

#def find_files(dir_name):
#    proc = ['find',
#            dir_name,
#            '-type',
#            'f']
#    p = subprocess.Popen(proc, stdout=subprocess.PIPE)
#    out, err = p.communicate()
#    return str(out, encoding='UTF-8').split()

def get_crawled_files(website_id):
    log_file_name = crawl_tmp_data_dir + '/' + website_id + '.log'
    files = []
    with open(log_file_name, encoding='latin-1') as f:
        for line in f:
            sp = line.split()
            if (re.match(date_regex, sp[0]) is not None and
                re.match(time_regex, sp[1]) is not None and
                sp[2].startswith('URL:') and
                sp[4] == '->'):
                url = sp[2][4:]
                file_name = sp[5][1:-1]
                if os.path.exists(file_name):
                    files.append( (url, file_name) )
    return files

def parse_file(f, website_run_id, website_id, page_id):
    soup = BeautifulSoup(open(f[1], 'rb'), "lxml", from_encoding='latin-1')
    if soup is None:
        return
    title = '' if soup.title is None else soup.title.string
    soup_get_text = soup.get_text()
    full_text = '' if soup_get_text is None else soup_get_text
    db.insert_page_data(website_run_id, page_id, title, full_text)
    find_all_a = soup.find_all('a')
    if find_all_a is not None:
        for a in find_all_a:
            a_href = a.get('href')
            href = '' if a_href is None else a_href.strip()
            a_strings = a.strings
            href_text = ( ''.join(a_strings).strip() if a_strings is not None
                          else '' )
            if (db.select_page_link(website_run_id, page_id, href)
                is None):
                db.insert_page_link(website_run_id, page_id, href, href_text)

def extract_data(db):
    website_run_id = db.insert_website_run()
    websites = db.select_website()
    for w in websites:
        website_id = str(w['website_id'])
        files = get_crawled_files(website_id)
        for f in files:
            print('> ' + str(f))
            page_url = f[0]
            if db.select_page(page_url) is None:
                page_id = db.insert_page(page_url, website_id)
                parse_file(f, website_run_id, website_id, page_id)
    db.end_website_run(website_run_id)

#make_directories()
#git_init()
db = SnacDatabase()
db.create_schema()
import_website_urls(db, 'interest_groups.txt')
#
#crawl(db)
#git_snapshot()
extract_data(db)
db.commit()

