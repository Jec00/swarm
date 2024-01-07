# Swarm
Swarm is the next generation of axiom, "The dynamic infrastructure framework for everybody!". 

This project refocusses the project to concentrate on distributed scanning with attack surface monitoring capabilities planned in the future.

When Swarm is fully functional, it should be able to:
- Orchestrate the dynamic scaling up/down of arbitary cloud hosts (AWS/Digital Ocean/Google Compute)
- Provide a reliable, trackable mechanism for long-term scanning operations
- Provide a database exposed via the HTTP API for querying of recon results
- Support scheduled scans
- Provide alerting capabilities (e.g new subdomains)

Swarm has three components, the server, the worker and the client.

## Server
The server acts as the command and control for all nodes, and orchestrates all operations via a HTTP REST API.

## Worker
The worker runs on each node, and connects to the server on a polling basis to retrieve jobs and process work.

## Client
The client connects to the server via the HTTP API and is the controller of actions performed within the swarm.
