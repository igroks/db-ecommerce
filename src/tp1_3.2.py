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


class Product(object):
    """Product class -> (asin, title, productGroup, salesrank)"""

    def __init__(self, asin, title=None, productGroup=None, salesrank=None):
        self.asin = asin
        self.title = title
        self.productGroup = productGroup
        self.salesrank = salesrank


class CommentsCatalog(object):
    """CommentsCatalog class -> (catAsin, total, downloaded, avgRating)"""

    def __init__(self, catAsin, total, downloaded, avgRating):
        self.catAsin = catAsin
        self.total = total
        self.downloaded = downloaded
        self.avgRating = avgRating


class Comments(object):
    """Comments class -> (commentAsin, idClient, date, rating, votes, helpful)"""

    def __init__(self, commentAsin, idClient, date, rating, votes, helpful):
        self.commentAsin = commentAsin
        self.idClient = idClient
        self.data = date
        self.rating = rating
        self.votes = votes
        self.helpful = helpful


class Similars(object):
    """Similars class -> (sAsin, asinSimilars)"""

    def __init__(self, sAsin, asinSimilars):
        self.sAsin = sAsin
        self.asinSimilars = asinSimilars


class Category(object):
    """Category class -> (name, id, fatherId)"""

    def __init__(self, name, id, fatherId):
        self.name = name
        self.id = id
        self.fatherId = fatherId


class Products_Per_Category(object):
    """Products_Per_Category class -> (ppcAsin, catId)"""

    def __init__(self, ppcAsin, catId):
        self.ppcAsin = ppcAsin
        self.catId = catId


def parseData():

    products = []
    with open('resources/amazon-meta-sample.txt') as f:
        lines = f.readlines()
        inBlock = ''
        product = {}
        product['product'] = ()
        categoriesSet = set()

        for line in lines:
            line = re.sub(r'\s{2,}?', ' ', line).replace('\n', '').strip()
            if not line and product:
                products.append(product)
                product = {}
                product['product'] = ()

            m = lineContent.match(line)

            if m and len(m.groups()) == 2:
                if m.group(1) == 'similar':
                    product['similar'] = m.group(2).split(' ')[1:]
                elif m.group(1) == 'categories':
                    inBlock = 'categories'
                    product['categories'] = []
                elif m.group(1) == 'reviews':
                    inBlock = 'reviews'
                else:
                    product['product'] += (m.group(2),)
            else:
                if inBlock == 'categories':
                    categoriesSet = set()
                    lineCategory = line.split('|')[1:]
                    for i in range(len(lineCategory)):
                        # individualCategory = re.search(
                        #     r"(.*)\[(\d+)\]", lineCategory[i])
                        if i > 0:
                            fatherCategory = re.search(
                                r"(.*)\[(\d+)\]", lineCategory[i-1])
                            childrenCategory = re.search(
                                r"(.*)\[(\d+)\]", lineCategory[i])
                            categoriesSet.add((childrenCategory.group(
                                1), childrenCategory.group(2), fatherCategory.group(2)))
                        else:
                            fatherCategory = re.search(
                                r"(.*)\[(\d+)\]", lineCategory[i])
                            categoriesSet.add((fatherCategory.group(
                                1), fatherCategory.group(2), None))

                    # product['categories'] = list(
                    #     set(product['categories'] + line.split('|')[1:]))
                    product['categories'] = list(categoriesSet)
                elif inBlock == 'reviews':
                    reviewsMatch = reviewsContent.match(line)
                    if not reviewsMatch:
                        print('Does not match!')
                    else:
                        date = f'{reviewsMatch.group(1)}-{reviewsMatch.group(2).rjust(2,"0")}-{reviewsMatch.group(3).rjust(2,"0")}'
                        if product.get('reviews') is None:
                            product['reviews'] = []
                        product['reviews'].append(
                            (
                                date,
                                reviewsMatch.group(4),
                                reviewsMatch.group(5),
                                reviewsMatch.group(6),
                                reviewsMatch.group(7)
                            )
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
