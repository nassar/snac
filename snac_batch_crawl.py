import time
import datetime
import subprocess
import snac

def subprocess_call(proc):
    """
    Call a subprocess and print a log message.
    """
    print('Running: ' + str(proc))
    subprocess.call(proc)

def call_retrieve_blog_rankings():
    """
    Run the retrieve_blog_rankings process.
    """
    proc = [ 'python3',
             'retrieve_blog_rankings.py' ]
    subprocess_call(proc)

def get_blog_rank_run():
    sql = """
        select blog_rank_run_id, start_time, end_time
        from blog_rank_run
        order by blog_rank_run_id desc
        limit 1;
        """
    conn = snac.connect_snac_db()
    try:
        cur = conn.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        cur.close()
        conn.commit()
        return row
    finally:
        conn.close()

def check_retrieve_blog_rankings(last_run):
    """
    Check if retrieve_blog_rankings completed.
    """
    print('Checking results')
    row = get_blog_rank_run()
    if row is None:
        print('Unexpected error: no runs found')
        return False
    (blog_rank_run_id, start_time, end_time) = row
    print('    Checking for new run')
    if last_run is not None and blog_rank_run_id == last_run[0]:
        print('New run not found')
        return False
    print('    Checking that run completed')
    if end_time is None:
        print('Missing end_time, i.e. run did not complete')
        return False
    print('OK')
    return True

def retrieve_blog_rankings():
    """
    Run blog ranking code, with retries if process fails.
    """
    while True:
        last_run = get_blog_rank_run()
        call_retrieve_blog_rankings()
        ok = check_retrieve_blog_rankings(last_run)
        if ok:
            break
        print('Sleep')
        time.sleep(60 * 60)    # Wait for one hour
        print('Retrying')

def get_blog_post_run():
    sql = """
        select blog_post_run_id, start_time, end_time
        from blog_post_run
        order by blog_post_run_id desc
        limit 1;
        """
    conn = snac.connect_snac_db()
    try:
        cur = conn.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        cur.close()
        conn.commit()
        return row
    finally:
        conn.close()

def call_retrieve_blog_posts():
    """
    Run the retrieve_blog_posts process.
    """
    proc = [ 'python3',
             'retrieve_blog_posts_map.py' ]
    subprocess_call(proc)

def check_retrieve_blog_posts(last_run):
    """
    Check if retrieve_blog_posts completed.
    """
    print('Checking results')
    row = get_blog_post_run()
    if row is None:
        print('Unexpected error: no runs found')
        return False
    (blog_post_run_id, start_time, end_time) = row
    print('    Checking for new run')
    if last_run is not None and blog_post_run_id == last_run[0]:
        print('New run not found')
        return False
    print('    Checking that run completed')
    if end_time is None:
        print('Missing end_time, i.e. run did not complete')
        return False
    print('OK')
    return True

def retrieve_blog_posts():
    """
    Run blog posts code, with retries if process fails.
    """
    while True:
        last_run = get_blog_post_run()
        call_retrieve_blog_posts()
        ok = check_retrieve_blog_posts(last_run)
        if ok:
            break
        print('Sleep')
        time.sleep(60 * 60)    # Wait for one hour
        time.sleep(60)
        print('Retrying')

def call_crawl_websites():
    """
    Run the crawl_websites process.
    """
    proc = [ 'python3',
             'crawl_websites.py' ]
    subprocess_call(proc)

def crawl_websites():
    """
    Run crawl websites code.
    """
    call_crawl_websites()

def print_datetime():
    print(str(datetime.datetime.now()))

def run_all():
    """
    Run all crawler scripts, with retries if process fails.
    """
    print_datetime()
    retrieve_blog_rankings()
    print_datetime()
    retrieve_blog_posts()
    print_datetime()
    crawl_websites()
    print_datetime()

run_all()

