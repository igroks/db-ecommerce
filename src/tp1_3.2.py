import os
import re
import psycopg2
from dotenv import load_dotenv

# if not os.path.isfile('resources/amazon-meta.txt'):
#     os.system('sh download-amazon-meta.sh')

load_dotenv('.env')
INPUTFILE = os.getenv('INPUT_FILE')
USER = os.getenv('POSTGRES_USER')
PASSWORD = os.getenv('POSTGRES_PASSWORD')
HOST = os.getenv('POSTGRES_HOST')
PORT = os.getenv('POSTGRES_DOCKER_PORT')
DATABASE = os.getenv('POSTGRES_DATABASE')

# lineContent = re.compile(r'^(?:  )?([A-Za-z]+):\s*(.+)$')
# reviewsContent = re.compile(
#     r'^    ([0-9]{4})-([0-9]{1,2})-([0-9]{1,2})\s+cutomer:\s+([A-Z0-9]+?)\s+rating:\s+([1-5])\s+votes:\s+([0-9]+?)\s+helpful:\s+([0-9]+)$')

lineContent = re.compile(r'^(?:    )?([A-Za-z]+):\s*(.+)$')
reviewsContent = re.compile(
    r'^([0-9]{4})-([0-9]{1,2})-([0-9]{1,2})\s+cutomer:\s+([A-Z0-9]+?)\s+rating:\s+([1-5])\s+votes:\s+([0-9]+?)\s+helpful:\s+([0-9]+)$')


def parseData():

    products = []
    with open('resources/amazon-meta-sample.txt') as f:
        lines = f.readlines()
        inBlock = ''
        product = {}

        for line in lines:
            line = re.sub(r'\s{2,}?', ' ', line).replace('\n', '').strip()
            if not line and product:
                products.append(product)
                product = {}

            m = lineContent.match(line)

            if m and len(m.groups()) == 2:
                if m.group(1) == 'similar':
                    product['similar'] = m.group(2).split('  ')[1:]
                elif m.group(1) == 'categories':
                    inBlock = 'categories'
                    product['categories'] = []
                elif m.group(1) == 'reviews':
                    inBlock = 'reviews'
                else:
                    product[m.group(1)] = m.group(2)
            else:
                if inBlock == 'categories':
                    product['categories'] = list(
                        set(product['categories'] + line.split('|')[1:]))
                elif inBlock == 'reviews':
                    reviewsMatch = reviewsContent.match(line)
                    if not reviewsMatch:
                        print('Does not match!')
                    else:
                        date = f'{reviewsMatch.group(1)}-{reviewsMatch.group(2).rjust(2,"0")}-{reviewsMatch.group(3).rjust(2,"0")}'
                        if product.get('reviews') is None:
                            product['reviews'] = []
                        product['reviews'].append(
                            {
                                'date': date,
                                'customer': reviewsMatch.group(4),
                                'rating': reviewsMatch.group(5),
                                'votes': reviewsMatch.group(6),
                                'helpful': reviewsMatch.group(7)
                            }
                        )
    return products


def dbConnect():

    conn = psycopg2.connect(
        host=HOST,
        database=DATABASE,
        user=USER,
        password=PASSWORD
    )

    cur = conn.cursor()
    script = open('sql_create_schema.txt', 'r')
    cur.execute(script.read())

    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":
    dbConnect()

    dbItens = parseData()
    for dbItem in dbItens:
        print(dbItem)
