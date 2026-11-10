# TEST FIXTURE ONLY: stand-in replica image. Runs no model; all output is SIMULATED.
# Build (from the project root):
#   docker build -t llmgw-standin:test -f tests/standin.Dockerfile tests
# Use: set `replica_image: llmgw-standin:test` in a test config; the ReplicaManager
# starts it with the same llama-server command line as the real image.
FROM python:3.12-slim
RUN pip install --no-cache-dir fastapi==0.162.2 uvicorn==0.54.0
COPY standin_replica.py /app/standin_replica.py
ENV STANDIN_STARTUP_DELAY=2 STANDIN_TOKEN_RATE=20
LABEL llmgw.kind="standin-simulated"
ENTRYPOINT ["python", "/app/standin_replica.py"]
CMD ["--host", "0.0.0.0", "--port", "8080"]
