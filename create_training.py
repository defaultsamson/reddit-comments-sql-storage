import sqlite3
from datetime import datetime

print_interval = 500000

f = open('timeframes.txt', 'r')
timeframes = f.read().splitlines()

for timeframe in timeframes:
    print('{}:[{}] Processing database...'.format(timeframe, str(datetime.now())))
    connection = sqlite3.connect('./reddit_db/{}.db'.format(timeframe))
    c = connection.cursor()

    c.execute("SELECT COUNT(*) FROM parent_reply GROUP BY parent_id")
    comment_count = c.fetchone()[0]
    
    row_counter = 0
    start_time = datetime.now()

    cur = c.execute("SELECT parent, comment FROM parent_reply")

    with open('train.from','a', encoding='utf8') as fro:
        with open('train.to','a', encoding='utf8') as to:
            for row in cur:
                fro.write(row[0] + '\n')
                to.write(row[1] + '\n')
                row_counter += 1
                if row_counter % print_interval == 0:
                    elapsed = (datetime.now() - start_time).total_seconds()
                    start_time = datetime.now()
                    rate = "NaN"
                    if elapsed != 0:
                        rate = round(10 * print_interval / elapsed) / 10 # Calculate the rate and round to the first decimal
                    print('{}:[{}] Rows Processed: {}/{}, Rate: {} Row/s'.format(timeframe, str(datetime.now()), row_counter, comment_count, rate))

