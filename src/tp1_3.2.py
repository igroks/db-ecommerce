import re
import os
import psycopg2
from dotenv import load_dotenv
from tqdm import tqdm

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
        date DATE NOT NULL,
        id_client TEXT NOT NULL,
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

products = []
categories = set()
similars = set()
reviews = []
productsByCategories = set()

def normalize(line):
    return ' '.join(line.split()).replace('\n','').strip()

def readDatasFromFile(file):
    attr = ''
    product = {}
   
    with open(file) as f:
        for line in tqdm(f.readlines(),'Reading file'):
            line = normalize(line)

            if not line and product:
                products.append(product)
                product = {}

            contentMatch = lineContentRegex.match(line)

            if contentMatch and len(contentMatch.groups()) == 2:
                attr, value = contentMatch.groups()
                if attr == 'similar':
                    similars.update([(product['asin'], similar) for similar in value.split(' ')[1:]])
                elif attr not in ['categories','reviews', 'Id']:
                    product[attr.lower()] = value  
            elif attr == 'categories':
                for category in line.split('|')[1:]:
                    name, id = categoryContentRegex.match(category).groups()
                    productsByCategories.add((product['asin'], id))
                    categories.add((id, name))
            elif attr == 'reviews':
                reviewsMatch = reviewsContentRegex.match(line)
                if reviewsMatch:
                    year, month, day = reviewsMatch.group(1,2,3)
                    reviews.append((
                        product['asin'],
                        f'{year}-{month.rjust(2,"0")}-{day.rjust(2,"0")}',
                        reviewsMatch.group(4),
                        reviewsMatch.group(5),
                        reviewsMatch.group(6),
                        reviewsMatch.group(7)
                    ))

if __name__ == '__main__':
    connection = None
    try:
        connection = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            database=os.getenv('POSTGRES_DATABASE'),
            port=os.getenv('POSTGRES_DOCKER_PORT')
        )
        
        print('Creating tables... ')
        cursor = connection.cursor()
        cursor.execute(tables)
        connection.commit()
        
        readDatasFromFile(os.getenv('INPUT_FILE'))

        for product in tqdm(products,'Insert into Product'):
            cursor.execute(
                'INSERT INTO Product (asin,title,product_group,salesrank) VALUES (%s,%s,%s,%s)',
                [product.get(arg) for arg in ['asin', 'title', 'group', 'salesrank']]
            )
        connection.commit()

        for similar in tqdm(similars,'Insert into Similars'):
            cursor.execute('INSERT INTO Similars (product_asin,similar_asin) VALUES (%s,%s)', similar)
        connection.commit()

        for review in tqdm(reviews,'Insert into Reviews'):
            cursor.execute(
                'INSERT INTO Reviews (product_asin,date,id_client,rating,votes,helpful) VALUES (%s,%s,%s,%s,%s,%s)',
                review
            )
        connection.commit()

        for category in tqdm(categories,'Insert into Category'):
            cursor.execute('INSERT INTO Category (id,name) VALUES (%s,%s)', category)
        connection.commit()

        for productAndCategory in tqdm(productsByCategories,'Insert into Products_per_category'): 
            cursor.execute('INSERT INTO Products_per_category (product_asin,category_id) VALUES (%s,%s)', productAndCategory)
        connection.commit()

        print('All inserts finished!')
    except (Exception, psycopg2.Error) as error:
        print('Error while connecting to PostgreSQL', error)
    finally:
        if connection:
            cursor.close()
            connection.close()
            print('PostgreSQL connection is closed')
