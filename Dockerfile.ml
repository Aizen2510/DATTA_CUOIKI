FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir "pip==24.0" "setuptools==69.5.1" "wheel==0.48.0" "numpy==1.26.4"

COPY requirements-ml.txt .
RUN pip install --no-cache-dir --no-use-pep517 --no-build-isolation -r requirements-ml.txt

COPY . .

ENTRYPOINT ["python", "recommend.py"]
CMD ["--help"]
