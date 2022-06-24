# db-ecommerce
Practical work of the Database subject at Universidade Federal do Amazonas which aims to design and implement a database on products sold in an e-commerce store, including user ratings and comments about these products. The work consists of creating a Relational Database containing data on product purchases and creating a Dashboard, a panel for monitoring purchase data, generating a series of reports. The input file from which the input data will be extracted will be the “Amazon product co-purchasing network metadata” that makes part of the Stanford Network Analysis Project (SNAP).

## Running All
If it's the first one you're running, you can run the entire build and run process using the command
```
make run-all
```
## Commands
To download the input file we can use the command
```
make download-amazon-meta
```
To upload services:
```
make run
```
To poulate database:
```
make populate-database
```
To execute the dashboard:
```
make start-dashboard
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
