#!/usr/bin/env python3

import os
import time
import datetime
from datetime import datetime
from flask import Flask, request, jsonify
import redis
import pymongo
import json
import boto3
import uuid
import threading
import requests
from botocore.exceptions import NoCredentialsError
from functools import wraps

S3_BUCKET = 'bucket_name'
app = Flask(__name__)
UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

worker_statuses = {}  # Dictionary to store worker statuses
job_statuses = {} # Dictionary to store job statuses

# DigitalOcean API token
API_TOKEN = ''

# Environment variables
SERVER_URL = ''
API_KEY = 'CHANGE_THIS'
AWS_ACCESS_KEY = ''
AWS_SECRET_KEY = ''
SNAPSHOT_NAME = 'DO_SNAPSHOT_NAME'

REQUESTS_PER_MINUTE = 250
RATE_LIMIT_INTERVAL = 60  # seconds

# Connect to Redis
redis_client = redis.Redis(host='127.0.0.1', port=6379, db=0)
# mongo
mongo_client = pymongo.MongoClient("localhost", 27017)

s3_client = boto3.client('s3')

def get_digitalocean_image_name(snapshot_name):
    headers = {
        'Authorization': f'Bearer {API_TOKEN}'
    }
    
    response = requests.get('https://api.digitalocean.com/v2/snapshots?per_page=200', headers=headers)
    if response.status_code == 200:
        snapshots = response.json().get('snapshots')
        snapshot = next((snapshot for snapshot in snapshots if snapshot['name'] == snapshot_name), None)

        return snapshot["id"]

SNAPSHOT_NAME = get_digitalocean_image_name(SNAPSHOT_NAME)

def get_droplets(prefix):
    headers = {
        'Authorization': f'Bearer {API_TOKEN}'
    }
    
    response = requests.get(f'https://api.digitalocean.com/v2/droplets?per_page=200', headers=headers)
    if response.status_code == 200:
        droplets = response.json().get('droplets', [])
        matching_droplets = [droplet for droplet in droplets if droplet['name'].startswith(prefix)]
        return matching_droplets
    else:
        return []



def generate_droplet_names(prefix, nodes):
    return [f'{prefix}{i}' for i in range(1, nodes + 1)]

def create_droplet(droplet_name):
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {API_TOKEN}'
    }
    user_data = f'''
    #cloud-config
    runcmd:
      - "docker run -d -e SERVER_URL={SERVER_URL} -e API_KEY={API_KEY} -e WORKER_ID={droplet_name} -e AWS_ACCESS_KEY={AWS_ACCESS_KEY} -e AWS_SECRET_KEY={AWS_SECRET_KEY} pry0cc/axiom-worker"
    '''
    data = {
        'name': droplet_name,
        'region': 'nyc3',
        'size': 's-1vcpu-1gb',
        'image': SNAPSHOT_NAME,
        'user_data': user_data
    }
    
    response = requests.post('https://api.digitalocean.com/v2/droplets?per_page=200', headers=headers, json=data)
    print(response.text)
    if response.status_code == 202:
        print(f'Droplet {droplet_name} creation initiated')
    else:
        print(f'Failed to create droplet {droplet_name}')

def create_droplets_concurrently(droplet_names):
    total_threads = 0
    start_time = time.time()

    for name in droplet_names:
        while total_threads >= REQUESTS_PER_MINUTE:
            elapsed_time = time.time() - start_time
            if elapsed_time < RATE_LIMIT_INTERVAL:
                sleep_time = RATE_LIMIT_INTERVAL - elapsed_time
                time.sleep(sleep_time)
                total_threads = 0
                start_time = time.time()
            else:
                total_threads -= REQUESTS_PER_MINUTE
                start_time = time.time()

        thread = threading.Thread(target=create_droplet, args=(name,))
        thread.start()
        total_threads += 1
    
    for thread in threading.enumerate():
        if thread != threading.current_thread():
            thread.join()

# Similar approach for delete_droplets_concurrently
def delete_droplet(droplet_id):
    headers = {
        'Authorization': f'Bearer {API_TOKEN}'
    }
    
    response = requests.delete(f'https://api.digitalocean.com/v2/droplets/{droplet_id}', headers=headers)
    if response.status_code == 204:
        print(f'Droplet {droplet_id} deleted')
    else:
        print(f'Failed to delete droplet {droplet_id}')

def delete_droplets_concurrently(droplets):
    total_threads = 0
    start_time = time.time()

    for droplet in droplets:
        while total_threads >= REQUESTS_PER_MINUTE:
            elapsed_time = time.time() - start_time
            if elapsed_time < RATE_LIMIT_INTERVAL:
                sleep_time = RATE_LIMIT_INTERVAL - elapsed_time
                time.sleep(sleep_time)
                total_threads = 0
                start_time = time.time()
            else:
                total_threads -= REQUESTS_PER_MINUTE
                start_time = time.time()

        thread = threading.Thread(target=delete_droplet, args=(droplet['id'],))
        thread.start()
        total_threads += 1
    
    for thread in threading.enumerate():
        if thread != threading.current_thread():
            thread.join()



def require_api_token(func):
    @wraps(func)
    def decorated_function(*args, **kwargs):
        expected_token = 'yoloswag'  # Replace with your actual API token
        provided_token = request.headers.get('Authorization')
        if not provided_token or not provided_token.startswith('Bearer '):
            return jsonify(message='Authentication required'), 401

        provided_token = provided_token.split(' ')[1]
        if provided_token != expected_token:
            return jsonify(message='Unauthorized'), 401

        return func(*args, **kwargs)
    return decorated_function

def generate_scan_id(module):
    timestamp = int(time.time())
    return f'{module}_{timestamp}'

def chunk_generator(sequence, batch_size):
    for i in range(0, len(sequence), batch_size):
        yield sequence[i:i + batch_size]

def update_redis_job(job_id, job):
    # Update Redis data here based on the provided information

    if job.get('started_at') is None:
        job['started_at'] = None
    
    if job.get('completed_at') is None:
        job['completed_at'] = None

    job_info = {
        'status': job["status"],
        'worker_id': job["worker_id"],
        'scan_id': job["scan_id"],
        'module':job["module"],
        'chunk_index':job["chunk_index"],
        'started_at':job["started_at"]
    }
    
    redis_client.hset('jobs', job_id, json.dumps(job_info))
    print(f'Updated Redis status for job {job_id}: {job_info}')
    return job_id

def queue_job_id(job_id):
    # Add job to the Redis queue here

    redis_client.rpush('job_queue', job_id)
    print(f'Queued job {job_id}')



@app.route('/get-statuses', methods=['GET'])
@require_api_token
def get_statuses():
    # Retrieve worker statuses from Redis
    worker_keys = redis_client.hkeys('workers')
    workers = {}
    for worker_key in worker_keys:
        worker_data = redis_client.hget('workers', worker_key)
        workers[worker_key.decode()] = json.loads(worker_data)

    # Retrieve job statuses from Redis
    job_keys = redis_client.hkeys('jobs')
    jobs = {}
    for job_key in job_keys:
        job_data = redis_client.hget('jobs', job_key)
        jobs[job_key.decode()] = json.loads(job_data)

    # Collate scan information
    scans = {}
    durations = []
    for job_id, job_info in jobs.items():
        scan_id = job_info.get('scan_id')
        if scan_id not in scans:
            scans[scan_id] = {
                'scan_id': scan_id,
                'total_chunks': 0,
                'chunks_complete': 0,
                'percent_complete': 0,
                'workers': [],
                'module': job_info.get('module'),
                'scan_started': None,
                'scan_completed': None,
                'completed_at': None,
                'scan_time': None,
                'scan_status': None,
                'average_scan_time': None
            }
        scans[scan_id]['total_chunks'] += 1
        if job_info.get('status') == 'complete':
            scans[scan_id]['chunks_complete'] += 1

        scans[scan_id]['percent_complete'] = round(scans[scan_id]['chunks_complete'] / scans[scan_id]['total_chunks'] * 100, 2)

        if job_info.get('worker_id') not in scans[scan_id]['workers']:
            scans[scan_id]['workers'].append(job_info.get('worker_id'))

        if scans[scan_id]['scan_started'] is None:
            scans[scan_id]['scan_started'] = int(scan_id.split('_')[1])

        if scans[scan_id]['completed_at'] is None:
            scans[scan_id]['completed_at'] = job_info.get('completed_at')
        elif job_info.get('completed_at') is not None:
            if job_info.get('completed_at') > scans[scan_id]['completed_at']:
                scans[scan_id]['completed_at'] = job_info.get('completed_at')

        if scans[scan_id]['percent_complete'] == 100:
            # read from mongo if it exists and update it, otherwise insert.

            asm = mongo_client["asm"]
            scans_collection = asm["scans"]

            query = {"scan_id": scan_id}
            scan = scans_collection.find_one(query)

            if scan is None:
                scan = {
                    "scan_id": scan_id,
                    "total_chunks": scans[scan_id]['total_chunks'],
                    "chunks_complete": scans[scan_id]['chunks_complete'],
                    "percent_complete": scans[scan_id]['percent_complete'],
                    "module": scans[scan_id]['module'],
                    "scan_started": scans[scan_id]['scan_started'],
                    "scan_completed": scans[scan_id]['completed_at'],
                    "scan_status": "complete"
                }
                scans_collection.insert_one(scan)



    # Collate and format the statuses
    status_summary = {
        'workers': workers,
        'jobs': jobs,
        'scans': list(scans.values())
    }

    return jsonify(status_summary), 200


@app.route('/update-job/<job_id>', methods=['POST'])
@require_api_token
def update_job(job_id):
    # get json object of changes
    changes = request.get_json()
    job = redis_client.hget('jobs', job_id)

    if job:
        # Decode the bytes to a dictionary
        job = json.loads(job.decode('utf-8'))

        # changes contains keys that need to be updated, a job_id
        for key, value in changes.items():
            if key in job and key is not None:
                job[key] = value

                if key == 'status' and value == 'complete':
                    job['completed_at'] = time.time()
                    redis_client.rpush('completed', job_id) 
                    

        # Update the job in redis after encoding it back to bytes
        redis_client.hset('jobs', job_id, json.dumps(job))
        print(f'Updated Redis status for job {job_id}: {job}')

        return jsonify(message='Job status updated'), 200
    else:
        return jsonify(message='Job not found'), 404

# path to get the chunk_id and read it from s3 and return it
@app.route('/get-chunk/<scan_id>/<chunk_id>', methods=['GET'])
@require_api_token
def get_chunk(scan_id, chunk_id):
    path = f'{scan_id}/output/chunk_{chunk_id}.txt'
    obj = s3_client.get_object(Bucket='bucket_name', Key=path)
    chunk_content_file = object_content = obj['Body'].read().decode('utf-8')

    return jsonify(contents=chunk_content_file), 200

# get latest chunk id
@app.route('/get-latest-chunk', methods=['GET'])
@require_api_token
def get_latest_chunk():
    latest_chunk_id = redis_client.lpop('completed')

    if latest_chunk_id is None:
        latest_chunk_id = ""

        return "", 204
    else:
        return latest_chunk_id, 200


# route to take job_id, read the file from s3 and then parse it to mongodb for the chunk_content
@app.route('/parse_job/<job_id>', methods=['GET'])
@require_api_token
def parse_job(job_id):
    # get the job from the job_id
    asm = mongo_client["asm"]
    jobs_collection = asm["jobs"]
    job = jobs_collection.find_one({'job_id': job_id})

    # get the scan_id and chunk_index
    scan_id = job['scan_id']
    chunk_index = job['chunk_index']

    chunk_filename = f'chunk_{scan_id}_{chunk_index}.txt'
    path = f'{scan_id}/output/{chunk_filename}'

    # get the chunk_content from s3
    obj = s3_client.get_object(Bucket='bucket_name', Key=path)
    chunk_content_file = object_content = obj['Body'].read().decode('utf-8')
    chunk_content = {}

    # parse the chunk_content to mongodb
    chunk_content['scan_id'] = scan_id
    chunk_content['chunk_index'] = chunk_index
    chunk_content['module'] = job['module']
    chunk_content['worker_id'] = job['worker_id']
    chunk_content['start_time'] = job['start_time']
    chunk_content['end_time'] = job['end_time']
    chunk_content['job_id'] = job_id
    chunk_content['content'] = chunk_content_file

    # insert the chunk_content into mongodb
    parsed_collection = asm[scan_id]
    parsed_collection.insert_one(chunk_content)

    return jsonify(message='Job parsed and inserted into mongodb'), 200

# route to read all the files in an s3 directory by scan_id and merge them into one file and return the contents
@app.route('/raw/<scan_id>', methods=['GET'])
@require_api_token
def get_raw_scan(scan_id):
    # get the list of files in the scan_id directory
    files = s3_client.list_objects(Bucket='bucket_name', Prefix=f'{scan_id}/output/')['Contents']
    files = [file['Key'] for file in files if file['Key'].endswith('.txt')]

    # read the contents of each file and merge them into one file
    contents = ""
    for file in files:
        obj = s3_client.get_object(Bucket='bucket_name', Key=file)
        contents += obj['Body'].read().decode('utf-8')

    return contents, 200

@app.route('/queue', methods=['POST'])
@require_api_token
def queue_job():
    job_data = request.get_json()
    print("JOB DATA:")
    print(job_data)
    module = job_data.get('module')
    
    if module is None:
        return 'Module must be provided', 400
    
    if job_data.get('scan_id') is None:
        job_data['scan_id'] = generate_scan_id(module)


    # create directories for input and output
    os.makedirs(os.path.join(app.config['UPLOAD_FOLDER'], job_data["scan_id"], 'input'), exist_ok=True)
    os.makedirs(os.path.join(app.config['UPLOAD_FOLDER'], job_data["scan_id"], 'output'), exist_ok=True)

    total_lines_in_chunk = len(job_data['file_content'])
    if job_data.get('batch_size') == 0:
        job_data['batch_size'] = total_lines_in_chunk

    for chunk_id, chunk in enumerate(chunk_generator(job_data['file_content'], job_data['batch_size'])):
        #if int(job_data.get('chunk_index')) == 0:
        job_data['chunk_index'] = chunk_id

        job_id = f'{job_data["scan_id"]}_{job_data["chunk_index"]}'
        chunk_filename = f'chunk_{job_data["chunk_index"]}.txt'
        chunk_path = os.path.join(job_data["scan_id"], 'input', chunk_filename)

    
        s3_key = os.path.join(job_data["scan_id"], 'input', chunk_filename)
        s3_client.put_object(Body='\n'.join(chunk), Bucket=S3_BUCKET, Key=s3_key)
    
        job = {
            'job_id': job_id,
            'scan_id': job_data['scan_id'],
            'chunk_index': job_data['chunk_index'],
            'module': module,
            'status': 'queued',
            'worker_id': None
        }

        update_redis_job(job_id, job)
        queue_job_id(job_id)
    
    return 'Job queued successfully', 200



@app.route('/get-job', methods=['GET'])
@require_api_token
def get_job():
    # update worker status
    worker_id = request.args.get('worker_id')
    print(worker_id)
    if worker_id not in worker_statuses:
        worker_statuses[worker_id] = {}

    worker_statuses[worker_id]['last_contact'] = time.time()
    redis_client.hset('workers', worker_id, json.dumps(worker_statuses[worker_id]))

    # use redis to get the next job
    popped_job_id = redis_client.lpop('job_queue')
    print(popped_job_id)

    if popped_job_id:
        job_data = json.loads(redis_client.hget('jobs', popped_job_id))
        print(job_data)

        job_data['status'] = 'in progress'
        job_data['started_at'] = time.time()
        job_data['worker_id'] = worker_id

        worker_statuses[worker_id]['polls_with_no_jobs'] = 0
        worker_statuses[worker_id]['status'] = 'active'

        update_redis_job(popped_job_id, job_data)
        redis_client.hset('workers', worker_id, json.dumps(worker_statuses[worker_id]))

        redis_client.hset('jobs', popped_job_id, json.dumps(job_data))

        return jsonify(job_data), 200
    else:
        if "polls_with_no_jobs" not in worker_statuses[worker_id]:
            worker_statuses[worker_id]['polls_with_no_jobs'] = 0
            worker_statuses[worker_id]['status'] = 'active'
        else:
            worker_statuses[worker_id]['polls_with_no_jobs'] += 1
            worker_statuses[worker_id]['status'] = 'pending'

        if worker_statuses[worker_id]['polls_with_no_jobs'] > 15:
            worker_statuses[worker_id]['status'] = 'inactive'
            redis_client.hset('workers', worker_id, json.dumps(worker_statuses[worker_id]))

            droplet_names = get_droplets(worker_id)
            x = threading.Thread(target=delete_droplets_concurrently, args=((droplet_names,)))
            x.start()


        return 'No jobs available', 204

@app.route('/spin-up', methods=['POST'])
@require_api_token
def spin_up_droplets():
    data = request.get_json()
    prefix = data.get('prefix')
    droplet_count = data.get('nodes')

    if prefix is None or droplet_count is None:
        return jsonify(message='Both prefix and nodes are required'), 400

    droplet_names = generate_droplet_names(prefix, droplet_count)

    x = threading.Thread(target=create_droplets_concurrently, args=((droplet_names,)))
    x.start()
    return jsonify(message=f'Spinning up {droplet_count} droplets with prefix {prefix}'), 202

@app.route('/spin-down', methods=['POST'])
@require_api_token
def spin_down_droplets():
    data = request.get_json()
    prefix = data.get('prefix')

    if prefix is None:
        return jsonify(message='Prefix is required'), 400

    droplet_names = get_droplets(prefix)
    x = threading.Thread(target=delete_droplets_concurrently, args=((droplet_names,)))
    x.start()

    return jsonify(message=f'Spinning down droplets with prefix {prefix}'), 202


# route to reset the redis database
@app.route('/reset', methods=['POST'])
@require_api_token
def reset():
    redis_client.flushall()
    return jsonify(message='Redis database reset'), 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)

