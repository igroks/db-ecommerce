FROM arielafonso/postgres:1.0

WORKDIR /app

RUN apt-get install -y python3

CMD ["python3","src/tp1_3.2.py"]
