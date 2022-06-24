import os
import psycopg2
import sys
from dotenv import load_dotenv

load_dotenv('.env')

def topTenComments(cursor):
    asin = input('Digite o ASIN do produto: ')
    cursor.execute(
        f'''
            SELECT * FROM Reviews ORDER BY rating DES, helpful DES WHERE product_asin = ({asin}) LIMIT 10;
        '''
    )
    for row in cursor.fetchall():
        print(row)

def bestSimilars(cursor):
    asin = input('Digite o ASIN do produto: ')
    cursor.execute(
        f'''
            SELECT * FROM product p INNER JOIN (SELECT similar_asin FROM similars WHERE product_asin ='{asin}')
            s ON p.asin = s.similar_asin WHERE p.salesrank > (SELECT salesrank FROM product WHERE asin = '{asin}');
        '''
    )
    for row in cursor.fetchall():
        print(row)

def showDailyEvolution(cursor):
    asin = input('Digite o ASIN do produto: ')
    cursor.execute(
        f'''
            SELECT date, AVG(rating) FROM reviews WHERE reviews.product_asin = '{asin}' GROUP BY date
            ORDER BY date ASC;
        '''
    )
    for row in cursor.fetchall():
        print(row)

def salesLeadersByGroups(cursor):
    cursor.execute(
        '''
            SELECT title, product_group FROM ( SELECT title, product_group, rank() OVER (PARTITION BY product_group ORDER BY salesrank DESC ) 
            FROM Product )Product WHERE RANK <=10 LIMIT 40;
        '''
    )
    for row in cursor.fetchall():
        print(row)

def bestEvaluated(cursor):
    cursor.execute(
        '''
            SELECT asin, title, product_group FROM product p INNER JOIN (SELECT product_asin, AVG(rating) rating, 
            AVG(helpful) helpful FROM reviews WHERE rating > 0 GROUP BY product_asin ORDER BY helpful DESC LIMIT 10) r
            ON p.asin = r.product_asin ORDER BY r.helpful DESC;
        '''
    )
    for row in cursor.fetchall():
        print(row)

if __name__ == "__main__":
    connection = None
    cursor = None
    try:
        connection = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            database=os.getenv('POSTGRES_DATABASE'),
            port=os.getenv('POSTGRES_DOCKER_PORT')
        )
        cursor = connection.cursor()

    except (Exception, psycopg2.Error) as error:
        print('Error while connecting to PostgreSQL', error)
    
    print('################################################################')
    print('Trabalho Prático 1 - Banco de Dados (2022)')
    print('################################################################')
    print('Opções de Dashboard:')
    print('A: Dado um produto, lista os 5 comentários mais úteis e com maior avaliação e os 5 comentários mais úteis e com menor avaliação.')
    print('B: Dado um produto, lista os produtos similares com maiores vendas do que ele.')
    print('C: Dado um produto, mostra a evolução diária das médias de avaliação ao longo do intervalo de tempo coberto no arquivo de entrada.')
    print('D: Lista os 10 produtos líderes de venda em cada grupo de produtos.')
    print('E: Lista os 10 produtos com a maior média de avaliações úteis positivas.')
    print('F: Lista as 5 categorias de produto com a maior média de avaliações úteis positivas por produto.')
    print('G: Lista os 10 clientes que mais fizeram comentários por grupo de produto.')
    print('ALL: Roda de forma iterativa todas as opções acima.')
    options = input('Digite as opções separadas por vírgulas para executar ou 0 para sair: ')
    print('################################################################')
    
    if options == '0': sys.exit(0)
    elif options.lower() == 'all': options = 'a,b,c,d,e,f,g'
    for option in options.lower().split(','):
        if option == 'a': pass
        elif option == 'b': bestSimilars(cursor)
        elif option == 'c': showDailyEvolution(cursor)
        elif option == 'd': salesLeadersByGroups(cursor)
        elif option == 'e': bestEvaluated(cursor)
        elif option == 'f': pass
        elif option == 'g': pass
    
    print('Thanks for using our program!')
    
    if connection:
        cursor.close()
        connection.close()
        print('PostgreSQL connection is closed')
        