
FROM python:3.8

WORKDIR /app
COPY pipeline.py pipeline_c.py 
COPY sales_data.csv sales_data.csv
RUN pip install pandas

ENTRYPOINT [ "python","pipeline_c.py" ]