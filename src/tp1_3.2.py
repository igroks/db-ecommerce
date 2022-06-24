import re
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv('.env')

lineContentRegex = re.compile(r'^(\w+):\s*(.+)$')
categoryContentRegex = re.compile(r'^(.*)\[(\d+?)\]$')
reviewsContentRegex = re.compile(r'^(\d{4})-(\d{1,2})-(\d{1,2}) \w+: (\w+) \w+: (\w+) \w+: (\w+) \w+: (\w+)$')
tables = '''
    CREATE TABLE IF NOT EXISTS Product( 
        asin TEXT PRIMARY KEY NOT NULL,
        title TEXT,
        product_group TEXT,
        salesrank INTEGER
    );
    CREATE TABLE IF NOT EXISTS Similars(
        product_asin TEXT NOT NULL,
        similar_asin TEXT NOT NULL,
        PRIMARY KEY(product_asin, similar_asin),
        FOREIGN KEY(product_asin) REFERENCES Product(asin)
    );
    CREATE TABLE IF NOT EXISTS Reviews(
        id SERIAL PRIMARY KEY NOT NULL,
        product_asin TEXT NOT NULL, 
        id_client TEXT NOT NULL,
        date DATE NOT NULL,
        rating INTEGER,
        votes INTEGER,
        helpful INTEGER,
        FOREIGN KEY(product_asin) REFERENCES Product(asin)
    );
    CREATE TABLE IF NOT EXISTS Category(
        id INTEGER PRIMARY KEY NOT NULL,
        name TEXT
    );
    CREATE TABLE IF NOT EXISTS Products_per_category(
        product_asin TEXT NOT NULL, 
        category_id INTEGER NOT NULL,
        PRIMARY KEY(product_asin, category_id),
        FOREIGN KEY(product_asin) REFERENCES Product(asin),
        FOREIGN KEY(category_id) REFERENCES Category(id)
    );
'''

def normalize(line):
    return ' '.join(line.split()).replace('\n','').strip()

def readDatasFromFile(file):
    products = []
    lines = []
    attr = ''
    product = {}
   
    with open(file) as f:
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
            elif attr not in ['categories','reviews', 'Id']:
                product[attr.lower()] = value  
        elif attr == 'categories':
            if attr not in product:
                product[attr] = set()
            product[attr].update(line.split('|')[1:])
        elif attr == 'reviews':
            reviewsMatch = reviewsContentRegex.match(line)
            if reviewsMatch:
                if attr not in product:
                    product[attr] = []
                year, month, day = reviewsMatch.group(1,2,3)
                product[attr].append({
                    'date': f'{year}-{month.rjust(2,"0")}-{day.rjust(2,"0")}',
                    'customer': reviewsMatch.group(4),
                    'rating': reviewsMatch.group(5),
                    'votes': reviewsMatch.group(6),
                    'helpful': reviewsMatch.group(7)
                })
    return products

if __name__ == '__main__':
    connection = None
    categories = set()
    try:
        connection = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            database=os.getenv('POSTGRES_DATABASE'),
            port=os.getenv('POSTGRES_DOCKER_PORT')
        )
        
        print('Creating tables...', end='') 
        cursor = connection.cursor()
        cursor.execute(tables)
        connection.commit()
        print('Concluded!')
        
        print('Reading file...', end='')
        products =  readDatasFromFile(os.getenv('INPUT_FILE'))
        print('Concluded!')

        print('Inserting datas in database, please wait...', end='')
        for product in products:
            cursor.execute(
                'INSERT INTO Product (asin,title,product_group,salesrank) VALUES (%s,%s,%s,%s)',
                [product.get(arg) for arg in ['asin', 'title', 'group', 'salesrank']]
            )
            
            for similar in (product.get('similars') or []):
                cursor.execute(
                    'INSERT INTO Similars (product_asin,similar_asin) VALUES (%s,%s)',
                    (product.get('asin'), similar)
                )

            for review in (product.get('reviews') or []):
                cursor.execute(
                    'INSERT INTO Reviews (product_asin,id_client,date,rating,votes,helpful) VALUES (%s,%s,%s,%s,%s,%s)',
                    [product.get('asin')] + [review.get(arg) for arg in ['customer', 'date', 'rating', 'votes', 'helpful']]
                )

            for category in (product.get('categories') or []):
                name, id = categoryContentRegex.match(category).groups()
                if id not in categories:
                    cursor.execute('INSERT INTO Category (id,name) VALUES (%s,%s)', (id, name))
                    categories.add(id)
                cursor.execute(
                    'INSERT INTO Products_per_category (product_asin,category_id) VALUES (%s,%s)',
                    (product.get('asin'), id)
                )
            
        connection.commit()
        print('Concluded!')
    except (Exception, psycopg2.Error) as error:
        print('Error while connecting to PostgreSQL', error)
    finally:
        if connection:
            cursor.close()
            connection.close()
            print('PostgreSQL connection is closed')
