from flask import Flask, request, send_file
from flask.ext.runner import Runner

app = Flask(__name__)
runner = Runner(app)

@app.route('/query_phrase_dates', methods=['GET'])
def query_phrase_dates():
    phrase = str(request.args.get('phrase'))
    date_start = str(request.args.get('date_start'))
    date_end = str(request.args.get('date_end'))
    sql = """
        select ...
        """
    data = (phrase, date_start, date_end)
    print(sql % data)
    conn = db_connect() #########
    cur = conn.cursor()
    cur.execute(sql, data)
    result = cur.fetchall()
    cur.close()
    conn.close()
    return result

