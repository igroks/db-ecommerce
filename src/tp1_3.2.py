import re
import os
import psycopg2
from psycopg2 import Error
from dotenv import load_dotenv

load_dotenv('.env')
inputFile = os.getenv('INPUT_FILE')
user = os.getenv('POSTGRES_USER')
password= os.getenv('POSTGRES_PASSWORD')
host = os.getenv('POSTGRES_HOST')
port = os.getenv('POSTGRES_DOCKER_PORT')
database = os.getenv('POSTGRES_DATABASE')

lineContentRegex = re.compile(r'^(?:)?([A-Za-z]+):\s*(.+)$')
reviewsContentRegex = re.compile(r'^([0-9]{4})-([0-9]{1,2})-([0-9]{1,2})\s+cutomer:\s+([A-Z0-9]+?)\s+rating:\s+([1-5])\s+votes:\s+([0-9]+?)\s+helpful:\s+([0-9]+)$')

def formatLine(line):
    return re.sub(r'\s{2,}?', ' ', line).replace('\n','').strip()

def readDatasFromFile():
    products = []
   
    with open(inputFile) as f:
        lines = f.readlines()
        attr = ''
        product = {}

        for line in lines:
            line = formatLine(line)

            if not line and product:
                products.append(product)
                product = {}

            contentMatch = lineContentRegex.match(line)

            if contentMatch and len(contentMatch.groups()) == 2:
                attr, value = contentMatch.groups()
                if attr == 'similar':
                    product[attr] = value.split('  ')[1:]
                elif attr in ['salesrank','Id']:
                    product[attr] = int(value)
                elif attr in ['categories', 'reviews']:
                    product[attr] = []
                else:
                    product[attr] = value
            elif attr == 'categories':
                product[attr] = list(set(product[attr] + line.split('|')[1:]))
            elif attr == 'reviews':
                reviewsMatch = reviewsContentRegex.match(line)
                if reviewsMatch:
                    year, month, day = reviewsMatch.group(1,2,3)
                    product[attr].append(
                        {
                            'date': f'{year}-{month.rjust(2,"0")}-{day.rjust(2,"0")}',
                            'customer': reviewsMatch.group(4),
                            'rating': int(reviewsMatch.group(5)),
                            'votes': int(reviewsMatch.group(6)),
                            'helpful': int(reviewsMatch.group(7))
                        }
                    )

def initDataBase():
    try:
        # Connect to an existing database
        connection = psycopg2.connect(user, password, host, port, database)

        # Create a cursor to perform database operations
        cursor = connection.cursor()
        # Print PostgreSQL details
        print('PostgreSQL server information')
        print(connection.get_dsn_parameters(), '\n')
        # Executing a SQL query
        cursor.execute('SELECT version();')
        # Fetch result
        record = cursor.fetchone()
        print('You are connected to - ', record, '\n')

    except (Exception, Error) as error:
        print('Error while connecting to PostgreSQL', error)
    finally:
        if (connection):
            cursor.close()
            connection.close()
            print('PostgreSQL connection is closed')

def create_tables():
    commands =( 
        """
        CREATE TABLE Product( 
            asin INTEGER PRIMARY KEY
            title TEXT
            group TEXT
            salesrank INTEGER
            similars INTEGER
        )
        """,
        """
        CREATE TABLE Comments_catalog(
            cat_asin INTEGER PRIMARY KEY
            total INTEGER
            downloaded INTEGER
            avg_rating INTEGER
            FOREIGN KEY(cat_asin)
            REFERENCES Product(asin)
        )
        """,
        """
        CREATE TABLE Comments(
            comment_asin INTEGER 
            id_client INTEGER 
            date DATE
            rating INTEGER
            votes INTEGER
            helpful INTEGER 
            PRIMARY KEY (comment_asin, id_client)
            FOREIGN KEY(comment_asin)
            REFERENCES Product(asin)
        )
        """,

        """
        CREATE TABLE Similars(
            s_asin INTEGER
            asin_similars INTEGER
            PRIMARY KEY(s_asin, asin_similars)
            FOREIGN KEY(s_asin)
            REFERENCES Product(asin)
        )"
        """,
        
        """        
        CREATE TABLE Category(
            id INTEGER PRIMARY KEY
            name TEXT
        )
        """,
        """
        CREATE TABLE Products_per_category(
            ppc_asin INTEGER 
            cat_id INTEGER
            FOREIGN KEY
        )
        """
    ) 
if __name__ == '__main__':
    readDatasFromFile()
    initDataBase()
