import sqlite3
import json
from datetime import datetime

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

parse_printing = 100000
match_printing = 1000
max_transactions = 100000
trash_interval = 10000

sql_transaction = []
def create_database(connection, c, timeframe):
    print("Creating Database: {}".format(timeframe))
    
    def create_table():
        c.execute("CREATE TABLE IF NOT EXISTS parent_reply(parent_id TEXT, comment_id TEXT UNIQUE, parent TEXT, comment TEXT, subreddit TEXT, unix INT, score INT)")

    def send_transaction():
        global sql_transaction
        if len(sql_transaction) > 0:
            start_time = datetime.now()
            c.execute('BEGIN TRANSACTION')
            for s in sql_transaction:
                try:
                    c.execute(s)
                except:
                    pass
            connection.commit()
            sql_transaction = []
            elapsed = (datetime.now() - start_time).total_seconds()
            print("Executed batch SQL transaction, took {} second(s)".format(round(elapsed * 100) / 100))
            return elapsed

    def build_transaction(sql):
        global sql_transaction
        sql_transaction.append(sql)

        if len(sql_transaction) >= max_transactions:
            return send_transaction()
        return 0

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
                if score >= 3 or score <= -3:
                    comment = acceptable(row['body'])
                    # Only comments with acceptable length and word count
                    if comment:
                        parent_id = row['parent_id'].split('_')[1]
                        comment_id = row['id']
                        subreddit = row['subreddit']
                        created_utc = row['created_utc']
                        
                        wasted_time += build_transaction("""INSERT INTO parent_reply (parent_id, comment_id, comment, subreddit, unix, score) VALUES ("{}","{}","{}","{}",{},{});""".format(parent_id, comment_id, comment, subreddit, int(created_utc), score))
                        
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
    send_transaction()

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
    #match_transaction = []

    c.execute("SELECT parent_id, comment_id, comment FROM parent_reply ORDER BY ABS(score) DESC")
    rows = c.fetchall()
    accepted_comments = len(rows)
    #print("len1: {}".format(accepted_comments))

    # A copy of the "rows" list used for finding children more quickly.
    # This is done by discarding the children from this list once found
    #c.execute("SELECT DISTINCT parent_id, comment_id, comment FROM parent_reply ORDER BY ABS(score) DESC")
    #c.execute("SELECT parent_id, comment_id, MAX(ABS(score)) FROM parent_reply GROUP BY parent_id")
    c.execute("SELECT parent_id, comment_id, MAX(ABS(score)) FROM parent_reply GROUP BY parent_id")
    rows_index = c.fetchall()
    index_len = len(rows_index)
    #print("len2: {}".format(len(rows_index)))
    #trash_index = []
    
    for row in rows:
        # Uses this comment's ID as the parent_id
        parent_id = row[1]
        row_counter += 1

        child = False
        for row2 in rows:
            if row2[0] == parent_id:
                child = row2
                break

        '''
        child = False
        for i in range(0, index_len - 1):
            row2 = rows_index[i]
            if row2[0] == parent_id:
                child = row2
                # Now remove the element
                if i == index_len - 1:
                    rows_index.pop()
                else:
                    rows_index[i] = rows_index.pop()
                    
                index_len -= 1
                break
                '''
        

        if child:
            c.execute("""UPDATE parent_reply SET parent=? WHERE comment_id=?;""", (row[2], child[1]))

        if row_counter % match_printing == 0:
            elapsed = (datetime.now() - start_time).total_seconds()
            start_time = datetime.now()
            rate = "NaN"
            if elapsed != 0:
                rate = round(10 * match_printing / elapsed) / 10
            print('{}:[{}] Rows Processed: {}/{}, Rate: {} Row/s'.format(timeframe, str(datetime.now()), row_counter, accepted_comments, rate))

    connection.commit()

for timeframe in timeframes:
    connection = sqlite3.connect('./reddit_db/{}.db'.format(timeframe))
    c = connection.cursor()
    create_database(connection, c, timeframe)
    match_parents(connection, c, timeframe)
    clean(connection, c)


