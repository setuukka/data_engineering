FROM python:3.12.3

WORKDIR /app

COPY requirements.txt .
COPY population_country_columns.csv .
COPY streamlit_population.py .

RUN pip install -r requirements.txt

CMD ["streamlit", "run", "streamlit_population.py"]