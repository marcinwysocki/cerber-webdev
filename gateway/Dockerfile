FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY contracts.py logs.py main.py export_contract.py ./
EXPOSE 8000
# --ws-max-size: ramki od klienta sa ignorowane, wiec 4096 B (domyslnie 16 MiB); limit dziala po dekompresji
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--ws-max-size", "4096"]
