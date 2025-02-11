FROM python:3.12-slim

WORKDIR /app
COPY mysqlcms1mil.py /app/
COPY 1_mil_records.csv /app/
RUN pip install pymysql pandas cryptography openpyxl

CMD ["python", "/app/mysqlcms1mil.py"]