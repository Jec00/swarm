FROM python:3.8-slim

# Set the working directory for the Python application
WORKDIR /app

# Copy your worker script, modules, and requirements
COPY worker.py /app/worker.py
COPY modules /app/modules
COPY requirements.txt /app/requirements.txt
COPY bin /app/bin
COPY artifacts /app/artifacts

RUN apt-get update && apt-get install -y nmap
# Install Python dependencies
RUN pip3 install -r requirements.txt
RUN mkdir -p /app/downloads/

# Set the entry point to accept command-line arguments and pass them as environment variables
CMD ["sh", "-c", "python3 worker.py --server-url $SERVER_URL --api-key $API_KEY --worker-id $WORKER_ID --aws-access-key $AWS_ACCESS_KEY --aws-secret-key $AWS_SECRET_KEY"]


