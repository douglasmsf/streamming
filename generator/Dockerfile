FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY cdc_generator.py .

# Roda continuamente gerando eventos CDC na pasta de landing
CMD ["python", "-u", "cdc_generator.py"]
