import re
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv('.env')
INPUT_FILE = os.getenv('INPUT_FILE')
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_DOCKER_PORT = os.getenv('POSTGRES_DOCKER_PORT')
POSTGRES_DATABASE = os.getenv('POSTGRES_DATABASE')

lineContentRegex = re.compile(r'^([A-Za-z]+):\s*(.+)$')
reviewsContentRegex = re.compile(
    r'^([0-9]{4})-([0-9]{1,2})-([0-9]{1,2})\s+cutomer:\s+([A-Z0-9]+?)\s+rating:\s+([1-5])\s+votes:\s+([0-9]+?)\s+helpful:\s+([0-9]+)$'
)
tables = [ 
    '''
    CREATE TABLE IF NOT EXISTS Product( 
        asin TEXT PRIMARY KEY,
        title TEXT,
        product_group TEXT,
        salesrank INTEGER,
        similars INTEGER
    )
    ''',
    '''
    CREATE TABLE IF NOT EXISTS Similars(
        s_asin TEXT,
        asin_similars TEXT,
        PRIMARY KEY(s_asin, asin_similars),
        FOREIGN KEY(s_asin) REFERENCES Product(asin)
    )
    ''',
    '''
    CREATE TABLE IF NOT EXISTS Reviews_catolog(
        cat_asin TEXT,
        total INTEGER,
        downloaded INTEGER,
        avg_rating INTEGER,
        PRIMARY KEY(cat_asin)
    )
    ''',
    '''
    CREATE TABLE IF NOT EXISTS Reviews(
        comment_asin TEXT, 
        id_client TEXT,
        date DATE,
        rating INTEGER,
        votes INTEGER,
        helpful INTEGER,
        PRIMARY KEY (comment_asin, id_client),
        FOREIGN KEY(comment_asin) REFERENCES Product(asin)
    )
    ''',
    '''        
    CREATE TABLE IF NOT EXISTS Category(
        id INTEGER PRIMARY KEY,
        name TEXT
    )
    ''',
    '''
    CREATE TABLE IF NOT EXISTS Products_per_category(
        ppc_asin TEXT PRIMARY KEY, 
        cat_id INTEGER
    )
    '''
]

attributeMap = {
    'Product':['ASIN', 'title', 'group', 'salesrank', 'num_similars'],
    'Reviews': ['ASIN', 'customer', 'date', 'rating', 'votes', 'helpful'] 
}

def normalize(line):
    return re.sub(r'\s{2,}?', ' ', line).replace('\n','').replace('\'','\'\'').strip()

def readDatasFromFile():
    products = []
    lines = None
    attr = ''
    product = {}
   
    with open(INPUT_FILE) as f:
        lines = f.readlines()

    for line in lines:
        line = normalize(line)

        if not line and product:
            products.append(product)
            product = {}

        contentMatch = lineContentRegex.match(line)

        if contentMatch and len(contentMatch.groups()) == 2:
            attr, value = contentMatch.groups()
            if attr == 'similar':
                value = value.split(' ')
                product['num_similars'] = value[0]
                product[attr] = value[1:]
            else:
                product[attr] = value if not attr in ['categories','reviews'] else []
        elif attr == 'categories':
            product[attr] = list(set(product[attr] + line.split('|')[1:]))
        elif attr == 'reviews':
            reviewsMatch = reviewsContentRegex.match(line)
            if reviewsMatch:
                year, month, day = reviewsMatch.group(1,2,3)
                product[attr].append({
                    'date': f'{year}-{month.rjust(2,"0")}-{day.rjust(2,"0")}',
                    'customer': reviewsMatch.group(4),
                    'rating': reviewsMatch.group(5),
                    'votes': reviewsMatch.group(6),
                    'helpful': reviewsMatch.group(7)
                })
    return products

def connectDataBase():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        database=POSTGRES_DATABASE,
        port=POSTGRES_DOCKER_PORT
    )

def executeCommand(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)
    connection.commit()
    cursor.close()

def closeDataBase(connection):
    if (connection):
        connection.close()
        print('PostgreSQL connection is closed')

def buidInsertQuery(table, args):
    return (
        f'INSERT INTO {table} VALUES {tuple(args)}'
        .replace('None','NULL')
        .replace('"','\'')
    )

if __name__ == '__main__':
    connection = None
    try:
        connection = connectDataBase()
        for table in tables:
            executeCommand(connection, table)

        for product in readDatasFromFile():
            executeCommand (
                connection, 
                buidInsertQuery('Product', [product.get(arg) for arg in attributeMap['Product']]),
            )
    except (Exception, psycopg2.Error) as error:
        print('Error while connecting to PostgreSQL', error)
    finally:
        closeDataBase(connection) 
