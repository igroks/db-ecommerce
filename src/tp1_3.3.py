import os
import psycopg2
from dotenv import load_dotenv
from tabulate import tabulate

load_dotenv('.env')
INPUTFILE = os.getenv('INPUT_FILE')
USER = os.getenv('POSTGRES_USER')
PASSWORD = os.getenv('POSTGRES_PASSWORD')
HOST = os.getenv('POSTGRES_HOST')
PORT = os.getenv('POSTGRES_DOCKER_PORT')
DATABASE = os.getenv('POSTGRES_DATABASE')


def usefulComment():
    print('A) os 5 comentários mais úteis e com maior avaliação e os 5 comentários mais úteis e com menor avaliação.\n')
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
    resultLabels = ['id', 'id_client', 'rating', 'votes', 'helpful']
    print(tabulate(result, headers=resultLabels))

    print('\n')


def similarMoreSales():
    conn = dbConnect()
    cur = conn.cursor()

    print('B) os 5 comentários mais úteis e com maior avaliação\n')
    queryString = """SELECT title
                     FROM  product p 
                     INNER JOIN (SELECT s_asin FROM similars WHERE p_asin ='0827229534') s
                        ON p.asin =  s.s_asin
                     WHERE  p.salesrank > (SELECT salesrank FROM product WHERE asin = '0827229534');
    """
    cur.execute(queryString)
    result = cur.fetchall()
    resultLabels = ['title']
    print(tabulate(result, headers=resultLabels))
    print(tabulate(result))

    print('\n')


def dailyEvolution():
    conn = dbConnect()
    cur = conn.cursor()

    print('C) a evolução diária das médias de avaliação ao longo do intervalo de tempo coberto no arquivo de entrada\n')
    queryString = """SELECT date, AVG(rating)
           FROM comments
           WHERE product_asin = '0231118597'
           GROUP BY date
           ORDER BY date ASC;"""
    cur.execute(queryString)
    result = cur.fetchall()
    resultLabels = ['date', 'AVG(rating)']
    print(tabulate(result, headers=resultLabels))

    print('\n')


def saleLeaders():
    conn = dbConnect()
    cur = conn.cursor()

    print('D) 10 produtos líderes de venda em cada grupo de produtos\n')
    queryString = """SELECT title 
                     FROM ( SELECT title, rank() OVER (PARTITION BY product_group ORDER BY salesrank DESC ) 
                     FROM Product ) Product WHERE RANK <=10 LIMIT 40;"""
    cur.execute(queryString)
    result = cur.fetchall()
    resultLabels = ['title']
    print(tabulate(result, headers=resultLabels))

    print('\n')


def higherReviewAvarageProduct():
    conn = dbConnect()
    cur = conn.cursor()

    print('E) 10 produtos com a maior média de avaliações úteis positivas por produto\n')
    queryString = """SELECT asin, title, product_group 
                     FROM product p
                     INNER JOIN 
                     (SELECT product_asin, AVG(rating) rating, AVG(helpful) helpful FROM comments WHERE rating > 0 GROUP BY product_asin ORDER BY helpful DESC LIMIT 10) r 
                        ON p.asin = r.product_asin
                     ORDER BY r.helpful DESC;"""
    cur.execute(queryString)
    result = cur.fetchall()
    resultLabels = ['asin', 'title', 'product_group']
    print(tabulate(result, headers=resultLabels))

    print('\n')


def higherReviewAvarageCategory():
    conn = dbConnect()
    cur = conn.cursor()

    print('F) 5 categorias de produto com a maior média de avaliações úteis positivas por produto\n')
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
    resultLabels = ['name']
    print(tabulate(result, headers=resultLabels))

    print('\n')


def clientMoreComments():
    conn = dbConnect()
    cur = conn.cursor()

    print('G) 10 clientes que mais fizeram comentários por grupo de produto\n')
    queryString = """SELECT * FROM (
                        SELECT *, ROW_NUMBER() OVER (PARTITION BY grupo ORDER BY comentarios DESC) AS n 
                        FROM (
                            SELECT p.product_group as grupo, c.id_client as cliente_id, COUNT(*) AS comentarios 
                            FROM Comments c
                            INNER JOIN Product p ON c.product_asin = p.asin
                            GROUP BY p.product_group, c.id_client
                            ORDER BY comentarios DESC
                            ) AS x
                        ) AS y
                     WHERE n <= 10;"""
    cur.execute(queryString)
    result = cur.fetchall()
    resultLabels = ['']
    print(tabulate(result, headers=resultLabels))

    print('\n')


def dbConnect():
    try:
        connection = psycopg2.connect(
            host=HOST,
            database=DATABASE,
            user=USER,
            password=PASSWORD
        )
    except (Exception, psycopg2.Error) as error:
        print('Error while connecting to PostgreSQL', error)

    return connection


def prompt():
    print("Bem-vindo ao Dashboard do TP1 de Banco de dados")
    print("Integrantes: Aldemir, Erlon e Glenn\n")
    print("Escolha uma da opções ou digite 'sair' para terminar o programa")
    print('*--------#--------#--------#--------#--------#--------*--------#--------#--------#--------#--------#--------*\n')
    print('A) Dado um produto, lista os 5 comentários mais úteis e com maior avaliação e os 5 comentários mais úteis e com menor avaliação.')
    print('B) Dado um produto, lista os produtos similares com maiores vendas do que ele.')
    print('C) Dado um produto, mostra a evolução diária das médias de avaliação ao longo do intervalo de tempo coberto no arquivo de entrada.')
    print('D) Lista os 10 produtos líderes de venda em cada grupo de produtos.')
    print('E) Lista os 10 produtos com a maior média de avaliações úteis positivas.')
    print('F) Lista as 5 categorias de produto com a maior média de avaliações úteis positivas por produto.')
    print('G) Lista os 10 clientes que mais fizeram comentários por grupo de produto.')
    print('T) Todas as consultas')
    flag = input(
        'Digite uma ou mais opções separadas por espaço (ex: a c e): ')
    print('\n')
    return flag


if __name__ == "__main__":

    flag = prompt()
    flag = flag.lower().split(' ')
    while('sair' not in flag):
        for f in flag:
            if f == 't':
                usefulComment()
                similarMoreSales()
                dailyEvolution()
                saleLeaders()
                higherReviewAvarageProduct()
                higherReviewAvarageCategory()
                clientMoreComments()
                flag = []
            else:
                if f == 'a':
                    usefulComment()
                elif f == 'b':
                    similarMoreSales()
                elif f == 'c':
                    dailyEvolution()
                elif f == 'd':
                    saleLeaders()
                elif f == 'e':
                    higherReviewAvarageProduct()
                elif f == 'f':
                    higherReviewAvarageCategory()
                elif f == 'g':
                    clientMoreComments()
                else:
                    continue

        flag = prompt()
