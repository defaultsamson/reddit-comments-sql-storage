## About
This project is an effort to improve the relevance of the information in [these database files](http://files.pushshift.io/reddit/comments/) while reducing the file size and improving usability via a SQL database.

This code was originally based off [a tutorial by sentdex](https://www.youtube.com/playlist?list=PLQVvvaa0QuDdc2k5dwtDTyT9aCja0on8j), and this project has pushed the performance and output consistency of this script to extremely efficient levels.

## Structure
The database should be sorted into folders by year
```
input
└───2011
│   │   2011-01
│   │   2011-02
│   │   ...
│   │   2011-12
│   
└───2012
│   │   2012-01
│   │   ...
...
```
The script will output to SQL databases in a flat file structure
```
output
│   2011-01.db
│   2011-02.db
│   ...
│   2011-12.db
│   2012-01.db
│   ...
```

## Options
```
-i <file>       Uses <file> as the input file (default: "timeframes.txt")
-m <mod>        Reads lines from input where the line number is in modulus <mod> (default: 1)
-r <remainder>  Uses <remainder> as the desired output of the modulus above (default: 0)
```

## Example Usage
With a given file structure
```
input
|   storage.py
│   time.txt
└───2011
│   │   2011-01
│   │   ...
│   │   2011-12
│   
└───2012
    │   2012-01
    │   ...
    │   2012-12
```
And the contents of `time.txt` are
```
2011-01
...
2011-12
2012-01
...
2012-12
```
To create a database for every row in the text file time.txt, one would execute
`./storage.py -i ./time.txt`
If you wanted to create a database for every third row, do 
`./storage.py -i ./time.txt -m 3`
If you wanted to create a database for every third row starting with the second row, do 
`./storage.py -i ./time.txt -m 3 -r 1`
Using the modulus and remainder parameters is handy when you'd like to run multiple processes at once. For example, to process three databases at a time, open 3 terminals and execute
`./storage.py -i ./time.txt -m 3 -r 0`
`./storage.py -i ./time.txt -m 3 -r 1`
`./storage.py -i ./time.txt -m 3 -r 2`

