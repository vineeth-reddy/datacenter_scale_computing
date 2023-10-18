
FROM python:3.11

WORKDIR /data
COPY pipeline.py pipeline.py
COPY animal_shelter.csv animal_shelter.csv
RUN pip install pandas sqlalchemy psycopg2
RUN python pipeline.py animal_shelter.csv

ENTRYPOINT ["python", "pipeline.py" ]