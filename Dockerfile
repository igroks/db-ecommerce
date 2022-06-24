FROM python:3.7

WORKDIR /app

COPY . .

RUN pip3 install psycopg2

RUN pip3 install python-dotenv

RUN pip3 install tqdm

RUN pip3 install tabulate
