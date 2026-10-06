Payment Generator
->
Python FastAPI
Payment Service
    │
    | -> PostgreSQL
    │
    | -> structured logs
Prometheus
->
Grafana
    |
    | Availability
    | Latency
    | Error Rate
    | Throughput

Instruction how to start the program: 
uvicorn traffic_generator.main:app --host 0.0.0.0 --port 8000 --reload
