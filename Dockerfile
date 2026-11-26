FROM python:3.11.10-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py schema.sql ./
ENV PYTHONUNBUFFERED=1
CMD ["python", "app.py", "stream"]
