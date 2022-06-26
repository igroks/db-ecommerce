import os
import sys
import psycopg2
from tabulate import tabulate
from dotenv import load_dotenv

load_dotenv('.env')

connection = None
cursor = None

def getAsin():
    asin = input('Digite o ASIN do produto: ')
    print()
    return asin 

def showResults(option, headers):
    print(f'[results for option {option}]:')
    print(tabulate(cursor.fetchall(), headers, tablefmt='psql'))
    print()

def topTenComments():
    asin = getAsin()
    cursor.execute(
        f'''
            (SELECT * FROM Reviews WHERE product_asin = '{asin}' ORDER BY rating DESC, helpful DESC LIMIT 5)
            UNION ALL (SELECT * FROM Reviews WHERE product_asin = '{asin}' ORDER BY rating ASC, helpful DESC LIMIT 5);
        '''
    )
    showResults('a',['ID', 'ASIN', 'DATE', 'CUSTOMER', 'RATING', 'VOTES', 'HELPFUL'])

def bestSimilars():
    asin = getAsin()
    cursor.execute(
        f'''
           SELECT title FROM  product p INNER JOIN (SELECT similar_asin FROM similars WHERE product_asin ='{asin}') s
           ON p.asin =  s.similar_asin WHERE  p.salesrank > (SELECT salesrank FROM product WHERE asin = '{asin}');
        '''
    )
    showResults('b',['TITLE'])

def showDailyEvolution():
    asin = getAsin()
    cursor.execute(
        f'''
            SELECT date, AVG(rating) FROM reviews WHERE review.product_asin = '{asin}' GROUP BY date
            ORDER BY date ASC;
        '''
    )
    showResults('c',['DATE','AVG_RATING'])

def salesLeadersByGroups():
    cursor.execute(
        '''
           SELECT title FROM ( SELECT *, rank() OVER (PARTITION BY product_group ORDER BY salesrank DESC ) 
           FROM Product)rank_group WHERE RANK <=10;
        '''
    )
    showResults('d',['TITLE'])

def bestEvaluated():
    cursor.execute(
        '''
            SELECT asin, title, product_group FROM product p INNER JOIN (SELECT product_asin, AVG(rating) rating, AVG(helpful) helpful FROM reviews WHERE rating > 0 GROUP BY product_asin ORDER BY helpful DESC LIMIT 10) r
            ON p.asin = r.product_asin ORDER BY r.helpful DESC;
        '''
    )
    showResults('e',['ASIN','TITLE','GROUP'])

def topCategoryByProduct():
    cursor.execute(
        '''
            SELECT name FROM category WHERE id IN (SELECT ppc.category_id FROM category c INNER JOIN products_per_category ppc
    	    ON c.id = ppc.category_id INNER JOIN reviews r ON r.product_asin = ppc.product_asin GROUP BY ppc.category_id 
            ORDER BY AVG(helpful) DESC LIMIT 5);
        '''
    )
    showResults('f',['NAME'])

def topClients():
    cursor.execute(
        '''
            SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY grupo ORDER BY reviews DESC) AS n FROM (
            SELECT p.product_group as grupo, c.id_client as cliente_id, COUNT(*) AS reviews FROM Reviews c INNER JOIN 
            Product p ON c.product_asin = p.asin GROUP BY p.product_group, c.id_client ORDER BY reviews DESC ) AS x) AS y
            WHERE n <= 10;
        '''
    )
    showResults('g',['GROUP','CUSTOMER','REVIEWS','ORDINATION'])

def menu():
    while True:
        print(
            tabulate(
                [
                    ['Opções do Dashboard:'],
                    ['A: Dado um produto, lista os 5 comentários mais úteis e com maior avaliação e os 5 comentários mais úteis e com menor avaliação.'],
                    ['B: Dado um produto, lista os produtos similares com maiores vendas do que ele.'],
                    ['C: Dado um produto, mostra a evolução diária das médias de avaliação ao longo do intervalo de tempo coberto no arquivo de entrada.'],
                    ['D: Lista os 10 produtos líderes de venda em cada grupo de produtos.'],
                    ['E: Lista os 10 produtos com a maior média de avaliações úteis positivas.'],
                    ['F: Lista as 5 categorias de produto com a maior média de avaliações úteis positivas por produto.'],
                    ['G: Lista os 10 clientes que mais fizeram comentários por grupo de produto.'],
                    ['ALL: Roda de forma iterativa todas as opções acima.']
                ], 
                ['Trabalho Prático 1 - Banco de Dados (2022)'], 
                tablefmt='rst'
            )
        )
        options = input('Digite as opções separadas por vírgulas para executar ou 0 para sair: ')
        print()
        
        if options == '0': close()
        elif options.lower() == 'all': options = 'a,b,c,d,e,f,g'
        for option in options.lower().split(','):
            if option == 'a': topTenComments()
            elif option == 'b': bestSimilars()
            elif option == 'c': showDailyEvolution()
            elif option == 'd': salesLeadersByGroups()
            elif option == 'e': bestEvaluated()
            elif option == 'f': topCategoryByProduct()
            elif option == 'g': topClients()
    
def close():
    print('Thanks for using our program!')
    
    if connection:
        cursor.close()
        connection.close()
        print('PostgreSQL connection is closed')
    print()
    sys.exit(0)

if __name__ == "__main__":    
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
    
    menu()
        