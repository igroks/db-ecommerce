import os
import re

if not os.path.isfile('resources/amazon-meta.txt'):
    os.system('sh download-amazon-meta.sh')

products = []
lineContent = re.compile(r'^(?:  )?([A-Za-z]+):\s*(.+)$')
reviewsContent = re.compile(r'^    ([0-9]{4})-([0-9]{1,2})-([0-9]{1,2})\s+cutomer:\s+([A-Z0-9]+?)\s+rating:\s+([1-5])\s+votes:\s+([0-9]+?)\s+helpful:\s+([0-9]+)$')

with open('resources/amazon-meta-test.txt') as f:
    lines = f.readlines()
    inBlock = ''
    product = {}

    for line in lines:
        line = line.replace('\n','')
        if not line and product:
            products.append(product)
            product = {}

        m = lineContent.match(line)

        if m and len(m.groups()) == 2:
            if m.group(1) ==  'similar':
                product['similar'] = m.group(2).split('  ')[1:]
            elif m.group(1) ==  'categories':
                inBlock = 'categories'
                product['categories'] = []
            elif m.group(1) == 'reviews':
                inBlock = 'reviews'
            else:
                product[m.group(1)] = m.group(2)
        else:
            if inBlock == 'categories':
                product['categories'] = list(set(product['categories'] + line.split('|')[1:]))
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
print(products)