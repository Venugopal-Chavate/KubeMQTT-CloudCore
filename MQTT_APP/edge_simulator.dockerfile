FROM python:3.9-slim
RUN addgroup --system appgroup && adduser --system --no-create-home --ingroup appgroup appuser
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY ./mqtt_edge_simulator.py .
RUN chown -R appuser:appgroup /app
USER appuser
CMD ["python", "mqtt_edge_simulator.py"]