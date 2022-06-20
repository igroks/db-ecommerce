FROM python:3.7

WORKDIR /app

COPY . .

RUN pip3 install psycopg2 python-dotenv

CMD ["python3","-u","src/tp1_3.2.py"]
