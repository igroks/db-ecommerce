import os
import re

inputFile = 'resources/amazon-meta-test.txt'

lineContentRegex = re.compile(r'^(?:  )?([A-Za-z]+):\s*(.+)$')
reviewsContentRegex = re.compile(r'^    ([0-9]{4})-([0-9]{1,2})-([0-9]{1,2})\s+cutomer:\s+([A-Z0-9]+?)\s+rating:\s+([1-5])\s+votes:\s+([0-9]+?)\s+helpful:\s+([0-9]+)$')

def downloadInputFile():
    if not os.path.isfile(inputFile):
        os.system('sh download-amazon-meta.sh')

def formatLine(line):
    return line.replace('\n','')
    
def readDatasFromFile():
    products = []

    with open(inputFile) as f:
        lines = f.readlines()
        attr = ''
        product = {}

        for line in lines:
            line = formatLine(line)

            if not line and product:
                products.append(product)
                product = {}

            contentMatch = lineContentRegex.match(line)

            if contentMatch and len(contentMatch.groups()) == 2:
                attr, value = contentMatch.groups()
                if attr == 'similar':
                    product[attr] = value.split('  ')[1:]
                elif attr in ['salesrank','Id']:
                    product[attr] = int(value)
                elif attr in ['categories', 'reviews']:
                    product[attr] = []
                else:
                    product[attr] = value
            else:
                if attr == 'categories':
                    product[attr] = list(set(product[attr] + line.split('|')[1:]))
                elif attr == 'reviews':
                    reviewsMatch = reviewsContentRegex.match(line)
                    if reviewsMatch:
                        year, month, day = reviewsMatch.group(1,2,3)
                        product[attr].append(
                            {
                                'date': f'{year}-{month.rjust(2,"0")}-{day.rjust(2,"0")}',
                                'customer': reviewsMatch.group(4),
                                'rating': int(reviewsMatch.group(5)),
                                'votes': int(reviewsMatch.group(6)),
                                'helpful': int(reviewsMatch.group(7))
                            }
                        )

if __name__ == '__main__':
    downloadInputFile()
    readDatasFromFile()
