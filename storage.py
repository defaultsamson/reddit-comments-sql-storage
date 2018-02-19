import sqlite3
import json
from datetime import datetime

parse_printing = 100000
match_printing = 10000
max_parses = 1000000
max_matches = 1000000

def getopts(argv):
    opts = {}  # Empty dictionary to store key-value pairs.
    while argv:  # While there are arguments left to parse...
        if argv[0][0] == '-':  # Found a "-name value" pair.
            opts[argv[0]] = argv[1]  # Add key and value to the dictionary.
        argv = argv[1:]  # Reduce the argument list by copying it starting from index 1.
    return opts

from sys import argv
args = getopts(argv)

filename = 'timeframes.txt'
if '-i' in args:
    filename = args['-i']

f = open(filename, 'r')
file_read = f.read().splitlines()
f.close()

mod = 1
if '-m' in args:
    mod = int(args['-m'])

remainder = 0
if '-r' in args:
    remainder = int(args['-r'])

timeframes = []
i = 0
for line in file_read:
    if i % mod == remainder:
        timeframes.append(line)
    i += 1

transaction = []
def send():
    global transaction
    if len(transaction) > 0:
        start_time = datetime.now()
        c.execute('BEGIN TRANSACTION')
        for s in transaction:
            try:
                c.execute(s)
            except:
                pass
        connection.commit()
        transaction = []
        elapsed = (datetime.now() - start_time).total_seconds()
        print("Executed batch SQL transaction, took {} second(s)".format(round(elapsed * 10) / 10))
        return elapsed

def build(maximum, sql):
    global transaction
    transaction.append(sql)

    if len(transaction) >= maximum:
        return send()
    return 0
    
def create_database(connection, c, timeframe):
    print("Creating Database: {}".format(timeframe))
    
    def create_table():
        c.execute("CREATE TABLE IF NOT EXISTS parent_reply(parent_id TEXT, comment_id TEXT UNIQUE, parent TEXT, comment TEXT, subreddit TEXT, unix INT, score INT)")

    def acceptable(body):
        body = body.split()
        if len(body) > 1000:
            return False
        body = ' '.join(body)
        if len(body) > 300 or len(body) < 1 or body == '[deleted]' or body == '[removed]':
            return False
        else:
            return body

    create_table()
    row_counter = 0
    acceptable_comments = 0
    start_time = datetime.now()
    wasted_time = 0

    with open("D:/Reddit Chatbot/reddit_data_decompressed/{}/RC_{}".format(timeframe.split('-')[0], timeframe), buffering=1000) as f:
        for row in f:
            row_counter += 1
            try:
                row = json.loads(row)
                score = row['score']

                # Only comments with an "interesting" score
                if score >= 2 or score <= -2:
                    comment = acceptable(row['body'])
                    # Only comments with acceptable length and word count
                    if comment:
                        parent_id = row['parent_id'].split('_')[1]
                        comment_id = row['id']
                        subreddit = row['subreddit']
                        created_utc = row['created_utc']
                        
                        wasted_time += build(max_parses, """INSERT INTO parent_reply (parent_id, comment_id, comment, subreddit, unix, score) VALUES ("{}","{}","{}","{}",{},{});""".format(parent_id, comment_id, comment, subreddit, int(created_utc), score))

                        acceptable_comments += 1
                        
            except Exception as e:
                print(str(e))
                
            if row_counter % parse_printing == 0:
                elapsed = (datetime.now() - start_time).total_seconds() - wasted_time
                start_time = datetime.now()
                wasted_time = 0
                rate = "NaN"
                if elapsed != 0:
                    rate = round(10 * parse_printing / elapsed) / 10
                print('{}:[{}] Acceptable Comments: {}/{}, Rate: {} Row/s'.format(timeframe, str(datetime.now()), acceptable_comments, row_counter, rate))

    # In case the transactions weren't sent during the loop
    send()

def clean(connection, c):
    start_time = datetime.now()

    c.execute("DELETE FROM parent_reply WHERE parent IS NULL")
    connection.commit()
    
    c.execute("VACUUM")
    connection.commit()
    
    elapsed = (datetime.now() - start_time).total_seconds()
    print("Cleaned up, took {} second(s)".format(round(elapsed * 10) / 10))

def match_parents(connection, c, timeframe):
    row_counter = 0
    start_time = datetime.now()
    wasted_time = 0
    
    # A list of all the top rated comments with unique parents
    c.execute("SELECT parent_id, comment_id, MAX(ABS(score)) FROM parent_reply GROUP BY parent_id")
    top_children_rows = c.fetchall()
    accepted_comments = len(top_children_rows)
    print("Traversing {} rows".format(len(top_children_rows)))
    
    for row in top_children_rows:
        row_counter += 1

        wasted_time += build(max_matches, """UPDATE parent_reply SET parent=(SELECT comment FROM parent_reply WHERE comment_id="{}") WHERE comment_id="{}";""".format(row[0], row[1]))

        if row_counter % match_printing == 0:
            elapsed = (datetime.now() - start_time).total_seconds() - wasted_time
            start_time = datetime.now()
            wasted_time = 0
            rate = "NaN"
            if elapsed != 0:
                rate = round(10 * match_printing / elapsed) / 10
            print('{}:[{}] Rows Processed: {}/{}, Rate: {} Row/s'.format(timeframe, str(datetime.now()), row_counter, accepted_comments, rate))

    send()

for timeframe in timeframes:
    connection = sqlite3.connect('./reddit_db/{}.db'.format(timeframe))
    c = connection.cursor()
    create_database(connection, c, timeframe)
    match_parents(connection, c, timeframe)
    clean(connection, c)


