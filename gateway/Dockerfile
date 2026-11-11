# Gateway runtime image for docker-compose.yml (Python + the gateway's runtime packages only).
# The source code is NOT copied in: docker-compose.yml bind-mounts the project directory at the
# same absolute path as on the host, so `models_dir: ./models` resolves to a path the Docker
# daemon can bind-mount into replica containers. Same pins as requirements.txt.
FROM python:3.12-slim
RUN pip install --no-cache-dir fastapi==0.162.2 uvicorn==0.54.0 httpx==0.28.1 pyyaml==6.0.3 docker==7.2.0
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
