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
args = getopts(argv) # Parses any command line parameters that were passed

# Parses the file name
filename = 'timeframes.txt'
if '-i' in args:
    filename = args['-i']

# Reads all the lines from the file
f = open(filename, 'r')
file_read = f.read().splitlines()
f.close()

mod = 1
if '-m' in args:
    mod = int(args['-m'])

remainder = 0
if '-r' in args:
    remainder = int(args['-r'])

# Uses timeframes as specified by the parameters
timeframes = []
i = 0
for line in file_read:
    if i % mod == remainder: # splits the file up into lines such that i % m == r
        timeframes.append(line)
    i += 1

transactions = []

# Executes a batch of sql commands
def send():
    global transactions
    if len(transactions) > 0:
        start_time = datetime.now()
        c.execute('BEGIN TRANSACTION')

        # Executes the transaction in a large batch
        for s in transactions:
            try:
                c.execute(s)
            except:
                pass
        connection.commit()
        transactions = [] # Clears the transaction

        # Prints and returns the elapsed time
        elapsed = (datetime.now() - start_time).total_seconds()
        print("Executed batch SQL transaction, took {} second(s)".format(round(elapsed * 10) / 10))
        return elapsed

# Adds a sql command to the batch of transactions
def build(maximum, sql):
    global transactions
    transactions.append(sql) # add the sql command to the transactions

    # Automatically sends the transaction if its length reaches a set maximum
    if len(transactions) >= maximum:
        return send()
    return 0

# Creates and populates the database
def create_database(connection, c, timeframe):
    print("Creating Database: {}".format(timeframe))

    # Creates the table
    def create_table():
        c.execute("CREATE TABLE IF NOT EXISTS parent_reply(parent_id TEXT, comment_id TEXT UNIQUE, parent TEXT, comment TEXT, subreddit TEXT, unix INT, score INT)")

    # Determines if a body of text is appropriate for use in the database
    def acceptable(body):
        body = body.split() # Removes all whitespace and new lines
        if len(body) > 300: # Must be less than 1000 words
            return False
        body = ' '.join(body) # Adds a single space back where there was once whitespace
        if len(body) > 300 or len(body) < 1 or body == '[deleted]' or body == '[removed]': # Must be less than 300 chars and more than 1, must not be deleted or removed
            return False
        else:
            return body

    create_table()
    
    row_counter = 0
    acceptable_comments = 0 # The number of comments added to the database
    start_time = datetime.now()
    wasted_time = 0 # The time wasted from building or sending

    with open("D:/Reddit Chatbot/reddit_data_decompressed/{}/RC_{}".format(timeframe.split('-')[0], timeframe), buffering=1000) as f:
        for row in f: # For every row in the above file ^
            row_counter += 1
            try:
                row = json.loads(row) # Load the JSON
                score = row['score']

                # Only comments with a score of interest
                if score >= 2 or score <= -2:
                    comment = acceptable(row['body'])
                    # Only comments with acceptable length and word count
                    if comment:
                        parent_id = row['parent_id'].split('_')[1]
                        comment_id = row['id']
                        subreddit = row['subreddit']
                        created_utc = row['created_utc']

                        # If it is acceptable, add it to the database
                        wasted_time += build(max_parses, """INSERT INTO parent_reply (parent_id, comment_id, comment, subreddit, unix, score) VALUES ("{}","{}","{}","{}",{},{});""".format(parent_id, comment_id, comment, subreddit, int(created_utc), score))

                        acceptable_comments += 1
                        
            except Exception as e:
                print(str(e))

            # If it's at the printing interval, output some information
            if row_counter % parse_printing == 0:
                elapsed = (datetime.now() - start_time).total_seconds() - wasted_time
                start_time = datetime.now()
                wasted_time = 0
                rate = "NaN"
                if elapsed != 0:
                    rate = round(10 * parse_printing / elapsed) / 10 # Calculate the rate and round to the first decimal
                print('{}:[{}] Acceptable Comments: {}/{}, Rate: {} Row/s'.format(timeframe, str(datetime.now()), acceptable_comments, row_counter, rate))

    # In case the transactions weren't sent during the loop
    send()


def clean(connection, c):
    start_time = datetime.now()

    # Removes all instances of comments with no parents
    # Ideally this shouldn't occur but because the comments are stored in discreet months there will be discrepancies
    c.execute("DELETE FROM parent_reply WHERE parent IS NULL OR comment IS NULL") 
    connection.commit() 

    # Packs the database to its smallest possible size
    c.execute("VACUUM") 
    connection.commit()

    # Print the elapsed time
    elapsed = (datetime.now() - start_time).total_seconds()
    print("Cleaned up, took {} second(s)".format(round(elapsed * 10) / 10))

def match_comments(connection, c, timeframe):
    row_counter = 0
    start_time = datetime.now()
    wasted_time = 0 # The time wasted from building or sending

    #c.execute("SELECT COUNT(*) FROM parent_reply GROUP BY parent_id")
    #accepted_comments = c.fetchone()[0]
    
    # A list of the top rated comment in each group of parent_id
    c.execute("SELECT parent_id, comment_id, MAX(ABS(score)) FROM parent_reply GROUP BY parent_id")
    top_children_rows = c.fetchall()
    accepted_comments = len(top_children_rows)
    print("Traversing {} rows".format(accepted_comments))
    
    for row in top_children_rows:
        row_counter += 1

        # Matches the child by finding the parent by its ID and filling in the "parent" field of the child
        wasted_time += build(max_matches, """UPDATE parent_reply SET parent=(SELECT comment FROM parent_reply WHERE comment_id="{}") WHERE comment_id="{}";""".format(row[0], row[1]))

        # If it's at the printing interval, output some information
        if row_counter % match_printing == 0:
            elapsed = (datetime.now() - start_time).total_seconds() - wasted_time
            start_time = datetime.now()
            wasted_time = 0
            rate = "NaN"
            if elapsed != 0:
                rate = round(10 * match_printing / elapsed) / 10 # Calculate the rate and round to the first decimal
            print('{}:[{}] Rows Processed: {}/{}, Rate: {} Row/s'.format(timeframe, str(datetime.now()), row_counter, accepted_comments, rate))

    send()

for timeframe in timeframes:
    connection = sqlite3.connect('./reddit_db/{}.db'.format(timeframe))
    c = connection.cursor()
    create_database(connection, c, timeframe)
    match_comments(connection, c, timeframe)
    clean(connection, c)


