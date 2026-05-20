FROM apache/airflow:3.2.1

# Installer les dépendances supplémentaires
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
