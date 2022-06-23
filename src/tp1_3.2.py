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
categoryContentRegex = re.compile(r'^(.*)\[([0-9]+?)\]$')
reviewsContentRegex = re.compile(
    r'^([0-9]{4})-([0-9]{1,2})-([0-9]{1,2})\s+cutomer:\s+([A-Z0-9]+?)\s+rating:\s+([1-5])\s+votes:\s+([0-9]+?)\s+helpful:\s+([0-9]+)$'
)
tables = [ 
    '''
    CREATE TABLE IF NOT EXISTS Product( 
        asin TEXT PRIMARY KEY,
        title TEXT,
        product_group TEXT,
        salesrank INTEGER
    )
    ''',
    '''
    CREATE TABLE IF NOT EXISTS Similars(
        product_asin TEXT,
        similar_asin TEXT,
        PRIMARY KEY(product_asin, similar_asin),
        FOREIGN KEY(product_asin) REFERENCES Product(asin)
    )
    ''',
    '''
    CREATE TABLE IF NOT EXISTS Reviews(
        id SERIAL PRIMARY KEY,
        product_asin TEXT, 
        id_client TEXT,
        date DATE,
        rating INTEGER,
        votes INTEGER,
        helpful INTEGER,
        FOREIGN KEY(product_asin) REFERENCES Product(asin)
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
        product_asin TEXT, 
        category_id INTEGER,
        PRIMARY KEY(product_asin, category_id),
        FOREIGN KEY(product_asin) REFERENCES Product(asin),
        FOREIGN KEY(category_id) REFERENCES Category(id)
    )
    '''
]

attributeMap = {
    'Product': ['asin', 'title', 'product_group', 'salesrank'],
    'Similars': ['product_asin', 'similar_asin'],
    'Reviews': ['product_asin', 'id_client', 'date', 'rating', 'votes', 'helpful'],
    'Category': ['id', 'name'],
    'Products_per_category': ['product_asin', 'category_id']
}

def normalize(line):
    return re.sub(r'\s{2,}?', ' ', line).replace('\n','').strip()

def readDatasFromFile():
    products = []
    categories = set()
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
                product['similars'] = value.split(' ')[1:]
            elif attr == 'group':
                product['product_group'] = value
            elif attr not in ['categories','reviews', 'Id']:
                product[attr.lower()] = value  
        elif attr == 'categories':
            if not product.get(attr):
                product[attr] = set()
            for category in line.split('|')[1:]:
                name, id = categoryContentRegex.match(category).groups()
                categories.add((id, name))
                product[attr].add(id)
        elif attr == 'reviews':
            reviewsMatch = reviewsContentRegex.match(line)
            if reviewsMatch:
                if not product.get(attr):
                    product[attr] = []
                year, month, day = reviewsMatch.group(1,2,3)
                product[attr].append({
                    'date': f'{year}-{month.rjust(2,"0")}-{day.rjust(2,"0")}',
                    'id_client': reviewsMatch.group(4),
                    'rating': reviewsMatch.group(5),
                    'votes': reviewsMatch.group(6),
                    'helpful': reviewsMatch.group(7)
                })
    return (products, categories)

def connectDataBase():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        database=POSTGRES_DATABASE,
        port=POSTGRES_DOCKER_PORT
    )

def executeCommand(connection, command, values=None):
    cursor = connection.cursor()
    cursor.execute(command, values)
    connection.commit()
    cursor.close()

def closeDataBase(connection):
    if (connection):
        connection.close()
        print('PostgreSQL connection is closed')

def buidInsertCommand(table, attributes):
    return 'INSERT INTO {} ({}) VALUES ({})'.format(
        table,
        ','.join(attributes),
        ','.join(['%s' for _ in range(len(attributes))])
    )

def insert(table, tuple):
    executeCommand (
        connection, 
        buidInsertCommand(table, attributeMap[table]),
        tuple
    )

if __name__ == '__main__':
    connection = None
    try:
        connection = connectDataBase()
        for table in tables:
            executeCommand(connection, table)
        
        products, categories = readDatasFromFile()

        for category in categories:
            insert('Category', category)

        for product in products:
            insert('Product', ([product.get(arg) for arg in attributeMap['Product']]))
            
            for similar in (product.get('similars') or []):
                insert('Similars', ([product.get('asin'), similar]))

            for review in (product.get('reviews') or []):
                insert('Reviews', ([product.get('asin')] + [review.get(arg) for arg in attributeMap['Reviews'][1:]]))

            for category in (product.get('categories') or []):
                insert('Products_per_category', (product.get('asin'), category))
    
    except (Exception, psycopg2.Error) as error:
        print('Error while connecting to PostgreSQL', error)
    finally:
        closeDataBase(connection) 
