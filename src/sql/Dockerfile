FROM python:3.11-slim
WORKDIR /app/queries
RUN pip install --no-cache-dir mysql-connector-python
COPY ./queries/ /app/queries/
CMD ["python", "run_dw_queries.py"]
