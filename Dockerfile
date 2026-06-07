# FINRA short-interest MCP connector — for public hosting (Render / Fly / Cloud Run).
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY server.py .
ENV MCP_TRANSPORT=http HOST=0.0.0.0
# Provide a FREE FINRA "Public Credential" at deploy time:
#   FINRA_API_CLIENT_ID=your_client_id
#   FINRA_API_CLIENT_SECRET=your_client_secret
# Optional overrides if FINRA renames the dataset/columns:
#   FINRA_SI_GROUP=otcMarket   FINRA_SI_DATASET=consolidatedShortInterest
#   FINRA_SI_SYMBOL_FIELD=issueSymbolIdentifier
# PORT is injected by the host; the server binds to it automatically.
CMD ["python", "server.py"]
