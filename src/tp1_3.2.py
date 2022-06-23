import os
import re
import psycopg2
from dotenv import load_dotenv

load_dotenv('.env')
INPUTFILE = os.getenv('INPUT_FILE')
USER = os.getenv('POSTGRES_USER')
PASSWORD = os.getenv('POSTGRES_PASSWORD')
HOST = os.getenv('POSTGRES_HOST')
PORT = os.getenv('POSTGRES_DOCKER_PORT')
DATABASE = os.getenv('POSTGRES_DATABASE')
categoriesSet = set()

lineContent = re.compile(r'^(?:    )?([A-Za-z]+):\s*(.+)$')
reviewsContent = re.compile(
    r'^([0-9]{4})-([0-9]{1,2})-([0-9]{1,2})\s+cutomer:\s+([A-Z0-9]+?)\s+rating:\s+([1-5])\s+votes:\s+([0-9]+?)\s+helpful:\s+([0-9]+)$')


class Product(object):
    """Product class -> (asin, title, productGroup, salesrank)"""

    def __init__(self, asin, title=None, product_group=None, salesrank=None):
        self.asin = asin
        self.title = title
        self.product_group = product_group
        self.salesrank = salesrank

    def __repr__(self) -> str:
        return f'{self.asin}, {self.title}, {self.product_group}, {self.salesrank}'


class Comments(object):
    """Comments class -> (product_asin, id_client, date, rating, votes, helpful)"""

    def __init__(self, product_asin, id_client, date, rating, votes, helpful):
        self.product_asin = product_asin
        self.id_client = id_client
        self.date = date
        self.rating = rating
        self.votes = votes
        self.helpful = helpful

    def __repr__(self) -> str:
        return f'{self.product_asin}, {self.id_client}, {self.date}, {self.rating}, {self.votes}, {self.helpful}'


class Similars(object):
    """Similars class -> (p_asin, s_asin)"""

    def __init__(self, p_asin, s_asin):
        self.p_asin = p_asin
        self.s_asin = s_asin


class Category(object):
    """Category class -> (name, id, father_id)"""

    def __init__(self, name, id, father_id):
        self.name = name
        self.id = id
        self.father_id = father_id


class Products_Per_Category(object):
    """Products_Per_Category class -> (ppc_asin, cat_id)"""

    def __init__(self, ppc_asin, cat_id):
        self.ppc_asin = ppc_asin
        self.cat_id = cat_id

    def __repr__(self) -> str:
        return f'{self.ppc_asin}, {self.cat_id}'


def parseData():

    products = []
    with open('resources/amazon-meta-sample.txt') as f:
        lines = f.readlines()
        inBlock = ''
        product = {}
        product['reviews'] = []
        product['categories'] = set()
        product['similar'] = []

        for line in lines:
            line = re.sub(r'\s{2,}?', ' ', line).replace('\n', '').strip()
            if not line and product:
                products.append(product)
                product = {}
                product['reviews'] = []
                product['categories'] = set()
                product['similar'] = []
                categoriesSet_tmp = set()

            m = lineContent.match(line)

            if m and len(m.groups()) == 2:
                if m.group(1) == 'similar':
                    product['similar'] = m.group(2).split(' ')[1:]
                elif m.group(1) == 'categories':
                    inBlock = 'categories'
                elif m.group(1) == 'reviews':
                    inBlock = 'reviews'
                else:
                    if m.group(1) == 'salesrank':
                        product[m.group(1)] = int(m.group(2))
                    else:
                        product[m.group(1)] = m.group(2)
            else:
                if inBlock == 'categories':
                    lineCategory = line.split('|')[1:]
                    for i in range(len(lineCategory)):
                        if i > 0:
                            fatherCategory = re.search(
                                r"(.*)\[(\d+)\]", lineCategory[i-1])
                            childrenCategory = re.search(
                                r"(.*)\[(\d+)\]", lineCategory[i])
                            categoriesSet_tmp.add((childrenCategory.group(
                                1), int(childrenCategory.group(2)), int(fatherCategory.group(2))))
                        else:
                            fatherCategory = re.search(
                                r"(.*)\[(\d+)\]", lineCategory[i])
                            categoriesSet_tmp.add((fatherCategory.group(
                                1), int(fatherCategory.group(2)), None))

                    categoriesSet.update(categoriesSet_tmp)
                    product['categories'].update(categoriesSet)
                elif inBlock == 'reviews':
                    reviewsMatch = reviewsContent.match(line)
                    if not reviewsMatch:
                        # print('Does not match!')
                        pass
                    else:
                        date = f'{reviewsMatch.group(1)}-{reviewsMatch.group(2).rjust(2,"0")}-{reviewsMatch.group(3).rjust(2,"0")}'
                        if product.get('reviews') is None:
                            product['reviews'] = []
                        product['reviews'].append(
                            {
                                'date': date,
                                'customer': reviewsMatch.group(4),
                                'rating': int(reviewsMatch.group(5)),
                                'votes': int(reviewsMatch.group(6)),
                                'helpful': int(reviewsMatch.group(7))
                            }
                        )
    return products


def queryString(className):
    p = tuple(className.__dict__.values())
    k = className.__dict__.keys()
    query_field_string = ', '.join(k)

    typeList = [type(x) for x in className.__dict__.values()]
    text = ''
    table = className.__class__.__name__
    insert_query = f"insert into {table} ({query_field_string}) values("

    for t in typeList:
        text += '\'{}\',' if t is str else '{},'

    text = text[:-1]
    unformatedQuery = insert_query + text + ')'
    formatedQuery = unformatedQuery.format(*p).replace(
        '\'None\'', 'NULL').replace('None', 'NULL')

    return formatedQuery


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

    dbItens = parseData()

    for categorie in categoriesSet:
        categories = Category(categorie[0], categorie[1], categorie[2])
        fraseCategories = queryString(categories)
        cur.execute(fraseCategories)

    for dbItem in dbItens[1:-1]:

        product = Product(dbItem.get('ASIN'), dbItem.get(
            'title'), dbItem.get('group'), dbItem.get('salesrank'))
        fraseProduct = queryString(product)
        cur.execute(fraseProduct)

        if dbItem.get('similar') is not None:
            for similar in dbItem['similar']:
                similars = Similars(dbItem.get('ASIN'), similar)
                fraseSimilar = queryString(similars)
                cur.execute(fraseSimilar)

        if dbItem.get('reviews') is not None:
            for comment in dbItem['reviews']:
                comments = Comments(dbItem.get('ASIN'), comment.get('customer'), comment.get('date'), comment.get(
                    'rating'), comment.get('votes'), comment.get('helpful'))
                fraseComment = queryString(comments)
                cur.execute(fraseComment)

        if dbItem.get('categories') is not None:
            for ppc in dbItem['categories']:
                categories_pp = Products_Per_Category(
                    dbItem.get('ASIN'), ppc[1])
                frasePpc = queryString(categories_pp)
                cur.execute(frasePpc)

    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":

    dbConnect()
