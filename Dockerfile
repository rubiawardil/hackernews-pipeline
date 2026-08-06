FROM apache/airflow:2.9.3-python3.11

# pip roda como usuário airflow; instalar como root quebra o site-packages
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
