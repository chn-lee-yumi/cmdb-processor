FROM python:3-alpine
WORKDIR /
RUN pip install pymysql kafka-python
COPY processor.py .
ENTRYPOINT ["python","processor.py"]