import sqlite3
import json
from datetime import datetime

f = open('timeframes.txt', 'r')
file_read = f.read().splitlines()
f.close()

timeframes = []
i = 0
for line in file_read:
    if i % 3 == 2 :
        timeframes.append(line)
    i += 1

parse_printing = 100000
match_printing = 10
max_transactions = 100000
max_match_transactions = 10000

sql_transaction = []
def create_database(connection, c, timeframe):
    print("Creating Database: {}".format(timeframe))
    
    def create_table():
        c.execute("CREATE TABLE IF NOT EXISTS parent_reply(paired BIT, parent_id TEXT, comment_id TEXT UNIQUE, parent TEXT, comment TEXT, subreddit TEXT, unix INT, score INT)")

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
                        
                        wasted_time += build_transaction("""INSERT INTO parent_reply (paired, parent_id, comment_id, comment, subreddit, unix, score) VALUES (0,"{}","{}","{}","{}",{},{});""".format(parent_id, comment_id, comment, subreddit, int(created_utc), score))
                        
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

    # SQLite doesn't support
    #c.execute("ALTER TABLE parent_reply DROP COLUMN paired")
    #connection.commit()
    
    c.execute("DELETE FROM parent_reply WHERE parent IS NULL")
    connection.commit()
    
    c.execute("VACUUM")
    connection.commit()
    
    elapsed = (datetime.now() - start_time).total_seconds()
    print("Cleaned up, took {} second(s)".format(round(elapsed * 10) / 10))

match_transaction = []
    
def match_parents(connection, c, timeframe):
    '''
    def send_match_transaction():
        global match_transaction
        if len(match_transaction) > 0:
            start_time = datetime.now()
            c.execute('BEGIN TRANSACTION')
            for s in match_transaction:
                try:
                    c.execute(s)
                except:
                    pass
            connection.commit()
            match_transaction = []
            elapsed = (datetime.now() - start_time).total_seconds()
            print("Executed batch SQL transaction, took {} second(s)".format(round(elapsed * 100) / 100))
            return elapsed

    def build_match_transaction(sql):
        global sql_transaction
        match_transaction.append(sql)

        if len(match_transaction) >= max_match_transactions:
            return send_match_transaction()
        return 0
        '''

    row_counter = 0
    start_time = datetime.now()
    #wasted_time = 0
    c.execute("SELECT COUNT(*) FROM parent_reply")
    accepted_comments = c.fetchone()[0]
    #match_transaction = []

    c.execute("CREATE INDEX IF NOT EXISTS idx_score ON parent_reply (ABS(score) DESC)")
    
    while True:
        # Selects the highest absolute scoring (to keep things interesting) unpaired comment
        sql1 = "SELECT comment_id, comment FROM parent_reply WHERE paired=0 ORDER BY ABS(score) DESC LIMIT 1"
        c.execute(sql1)
        parent = c.fetchone()
        # If there are no more comments left to traverse
        if parent == None:
            break
        parent_id = parent[0]
        parent_body = parent[1]

        # Then locates the highest ranking child of the above parent comment
        sql2 = """SELECT comment_id FROM parent_reply WHERE parent_id='{}' ORDER BY ABS(score) DESC LIMIT 1""".format(parent_id)
        c.execute(sql2)
        comment = c.fetchone()
        # Just double checking that the child exists
        if comment != None:
            comment_id = comment[0]

            # Then fills the "parent" field of the child comment
            c.execute("""UPDATE parent_reply SET parent=? WHERE comment_id=?;""", (parent_body, comment_id))
            #build_match_transaction("""UPDATE parent_reply SET parent="{}" WHERE comment_id="{}";""".format(parent_body, comment_id))
            
        # Then updates the "paired" field of the parent comment so that it's not traversed again
        sql3 = """UPDATE parent_reply SET paired=1 WHERE comment_id='{}'""".format(parent_id)
        #build_match_transaction(sql3)
        
        c.execute(sql3)
        # TODO Don't commit as often. Keep track of parent id's?
        connection.commit()

        row_counter += 1

        # This could remove potential parents of other comments, so don't run.
        # Instead just clean at the end
        #c.execute("""DELETE FROM parent_reply WHERE parent_id='{}' AND NOT comment_id='{}'""".format(parent_id, comment_id))
        #connection.commit()
        
        if row_counter % match_printing == 0:
            elapsed = (datetime.now() - start_time).total_seconds() #- wasted_time
            start_time = datetime.now()
            #wasted_time = 0
            rate = "NaN"
            if elapsed != 0:
                rate = round(10 * match_printing / elapsed) / 10
            print('{}:[{}] Rows Processed: {}/{}, Rate: {} Row/s'.format(timeframe, str(datetime.now()), row_counter, accepted_comments, rate))

    #send_match_transaction()

for timeframe in timeframes:
    connection = sqlite3.connect('./reddit_db/{}.db'.format(timeframe))
    c = connection.cursor()
    create_database(connection, c, timeframe)
    match_parents(connection, c, timeframe)
    clean(connection, c)


