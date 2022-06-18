import os

if not os.path.isfile('resources/amazon-meta.txt'):
    os.system('sh download-amazon-meta.sh')
