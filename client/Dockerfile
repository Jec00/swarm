# Use an official Python runtime as a parent image
FROM python:3.8-slim

# Set the working directory in the container
WORKDIR /app

# Copy the server script and any other necessary files to the container
COPY swarm /app/swarm
COPY requirements.txt /app/requirements.txt

# Install any dependencies required by your server.py script (if any)
RUN pip install -r requirements.txt  # Example dependency


# Define the command to run your server.py script
ENTRYPOINT ["python", "swarm"]

