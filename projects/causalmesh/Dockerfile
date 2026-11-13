FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 CAUSALMESH_DB=/app/data/causalmesh.db
WORKDIR /app
COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir .
RUN useradd -u 10001 -m causalmesh && mkdir -p /app/data && chown causalmesh:causalmesh /app/data
USER causalmesh
EXPOSE 8000
CMD ["uvicorn", "causalmesh.api:app", "--host", "0.0.0.0", "--port", "8000"]
