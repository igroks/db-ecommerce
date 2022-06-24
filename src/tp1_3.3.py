import os
import psycopg2
from dotenv import load_dotenv

load_dotenv('../.env')
INPUTFILE = os.getenv('INPUT_FILE')
USER = os.getenv('POSTGRES_USER')
PASSWORD = os.getenv('POSTGRES_PASSWORD')
HOST = os.getenv('POSTGRES_HOST')
PORT = os.getenv('POSTGRES_DOCKER_PORT')
DATABASE = os.getenv('POSTGRES_DATABASE')


def usefulComment():
    print('os 5 comentários mais úteis e com maior avaliação e os 5 comentários mais úteis e com menor avaliação.\n')
    conn = dbConnect()
    cur = conn.cursor()
    # queryString = """(SELECT *
    #                   FROM Comments
    #                   WHERE product_asin = '0231118597' AND rating > 3 ORDER BY helpful DESC LIMIT 5);"""
    queryString = """(SELECT id, id_client, rating, votes, helpful 
                      FROM comments JOIN product on product_asin = product.asin 
                      WHERE product.asin='0738700797' 
                      ORDER BY rating, helpful desc limit 5) UNION ALL
                      (SELECT id, id_client, rating, votes, helpful 
                      FROM comments JOIN product on product_asin = product.asin 
                      WHERE product.asin='0738700797' 
                      ORDER BY rating, helpful desc limit 5);"""
    cur.execute(queryString)
    result = cur.fetchall()
    l = []
    columNames = [column[0] for column in cur.description]
    for row in result:
        l.append(dict(zip(columNames, row)))
    for value in l:
        print(value)


def similarMoreSales():
    conn = dbConnect()
    cur = conn.cursor()

    print('os 5 comentários mais úteis e com maior avaliação\n')
    queryString = """SELECT title
                     FROM  product p 
                     INNER JOIN (SELECT s_asin FROM similars WHERE p_asin ='0827229534') s
                        ON p.asin =  s.s_asin
                     WHERE  p.salesrank > (SELECT salesrank FROM product WHERE asin = '0827229534');
    """
    cur.execute(queryString)
    result = cur.fetchall()
    for row in result:
        print(row)

    print('\n')


def dailyEvolution():
    conn = dbConnect()
    cur = conn.cursor()

    print('a evolução diária das médias de avaliação ao longo do intervalo de tempo coberto no arquivo de entrada\n')
    queryString = """SELECT date, AVG(rating)
           FROM comments
           WHERE product_asin = '0231118597'
           GROUP BY date
           ORDER BY date ASC;"""
    cur.execute(queryString)
    result = cur.fetchall()
    for row in result:
        print(row)

    print('\n')


def saleLeaders():
    conn = dbConnect()
    cur = conn.cursor()

    print('10 produtos líderes de venda em cada grupo de produtos\n')
    queryString = """SELECT title 
                     FROM ( SELECT title, rank() OVER (PARTITION BY product_group ORDER BY salesrank DESC ) 
                     FROM Product ) Product WHERE RANK <=10 LIMIT 40;"""
    cur.execute(queryString)
    result = cur.fetchall()
    for row in result:
        print(row)

    print('\n')


def higherReviewAvarageProduct():
    conn = dbConnect()
    cur = conn.cursor()

    print('10 produtos com a maior média de avaliações úteis positivas por produto\n')
    queryString = """SELECT asin, title, product_group 
                     FROM product p
                     INNER JOIN 
                     (SELECT product_asin, AVG(rating) rating, AVG(helpful) helpful FROM comments WHERE rating > 0 GROUP BY product_asin ORDER BY helpful DESC LIMIT 10) r 
                        ON p.asin = r.product_asin
                     ORDER BY r.helpful DESC;"""
    cur.execute(queryString)
    result = cur.fetchall()
    for row in result:
        print(row)

    print('\n')


def higherReviewAvarageCategory():
    conn = dbConnect()
    cur = conn.cursor()

    print('5 categorias de produto com a maior média de avaliações úteis positivas por produto\n')
    queryString = """SELECT name 
                     FROM category
                     WHERE id IN (SELECT ppc.cat_id
	                 FROM category c
	                 INNER JOIN products_per_category ppc 
                        ON c.id = ppc.cat_id
                     INNER JOIN comments r 
                        ON r.product_asin = ppc.ppc_asin
                     GROUP BY ppc.cat_id
                     ORDER BY AVG(helpful) DESC 
                     LIMIT 5);
"""
    cur.execute(queryString)
    result = cur.fetchall()
    for row in result:
        print(row)

    print('\n')


def clientMoreComments():
    conn = dbConnect()
    cur = conn.cursor()

    print('10 clientes que mais fizeram comentários por grupo de produto\n')
    queryString = """SELECT rank_clients.id_client
                     FROM ( SELECT p.*, r.*,
                     rank() OVER (
                     PARTITION BY p.product_group 
                     ORDER BY COUNT(p.product_group) DESC ) 
                     FROM Product p, Comments r) rank_clients 
                     WHERE RANK <=10"""
    cur.execute(queryString)
    result = cur.fetchall()
    for row in result:
        print(row)

    print('\n')


def dbConnect():

    return psycopg2.connect(
        host=HOST,
        database=DATABASE,
        user=USER,
        password=PASSWORD
    )


if __name__ == "__main__":

    usefulComment()

    # similarMoreSales()

    # dailyEvolution()

    # saleLeaders()

    # higherReviewAvarageProduct()

    # higherReviewAvarageCategory()

    # clientMoreComments()
