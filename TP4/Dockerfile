FROM python:3.12-slim
WORKDIR /app
COPY libs.txt .
RUN pip install --no-cache-dir -r libs.txt
COPY app.py .
COPY data/HI-Large_Trans.csv .
CMD ["python", "app.py"]