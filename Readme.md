# db-ecommerce
Practical work of the Database subject at Universidade Federal do Amazonas which aims to design and implement a database on products sold in an e-commerce store, including user ratings and comments about these products. The work consists of creating a Relational Database containing data on product purchases and creating a Dashboard, a panel for monitoring purchase data, generating a series of reports.

The input file from which the input data will be extracted will be the “Amazon product co-purchasing network metadata” that makes
part of the Stanford Network Analysis Project (SNAP). However, to make it easier we have already made available a sample file with the first 100 products in `resources/sample.txt`

## Running
If you want to use the complete file, download it using the command below. In addition you must change the value of the `INPUT_FILE` environment variable in the `.env` file to `resources/amazon-meta.txt`.
```
sh scripts/download-amazon-meta.sh
```
We can start using the command:
```
make run
```
To stop:
```
make stop
```

## Authors
**Student:** Bruna Mariana Ferreira de Souza  
**Email:** bruna.mariana@icomp.ufam.edu.br

**Student:** Igor Carvalho da Silva  
**Email:** igor.carvalho@icomp.ufam.edu.br

**Student:** Thayná Rosa Silvestre   
**Email:** thayna.rosa@icomp.ufam.edu.br
