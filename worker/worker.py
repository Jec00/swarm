#!/usr/bin/env python3

import requests
import json
import os
import time
import subprocess
import argparse
import concurrent.futures
import threading

import boto3
from botocore.exceptions import NoCredentialsError


class JobProcessor:
    def __init__(self, server_url, api_key, worker_id, max_jobs, aws_access_key, aws_secret_key):
        self.server_url = server_url
        self.api_key = api_key
        self.worker_id = worker_id
        self.max_jobs = max_jobs
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key


    
    def get_module_cmd(self, module_name, file_name, output_filename):
        with open('modules/' + module_name + '.json', 'r') as f:
            module = json.load(f)
            command = module['command']
            command = command.replace('{input}', file_name)
            command = command.replace('{output}', output_filename)
            return command
    

    def get_job(self):
        headers = {'Authorization': f'Bearer {self.api_key}'}
        response = requests.get(f'{self.server_url}/get-job?worker_id={self.worker_id}', headers=headers)
        return response.json() if response.status_code == 200 else None

    def update_worker_status(self):
        headers = {'Authorization': f'Bearer {self.api_key}', 'Content-Type': 'application/json'}
        data = {'last_contact': int(time.time())}
        response = requests.post(f'{self.server_url}/update-worker/{self.worker_id}', headers=headers, json=data)
        return response.status_code

    def update_job_status(self, job_id, changes):
        headers = {'Authorization': f'Bearer {self.api_key}', 'Content-Type': 'application/json'}
        response = requests.post(f'{self.server_url}/update-job/{job_id}', headers=headers, json=changes)

        if response.status_code == 200:
            print(f'Updated job status for {job_id}')
        else:
            print(f'Failed to update job status for {job_id}, response: {response.text}')


    def process_chunk(self, job_data):
        job_id = f'{job_data["scan_id"]}_{job_data["chunk_index"]}'
        job_data['job_id'] = job_id
        print(f"Processing job: {job_data}")
        self.update_job_status(job_data['job_id'], changes={'status': 'starting'})
        subprocess.run("mkdir -p downloads", shell=True)
    
        chunk_index = job_data['chunk_index']
        scan_id = job_data['scan_id']
        chunk_filename = f'chunk_{chunk_index}.txt'
        input_file = os.path.join('downloads', chunk_filename)  # Save to 'downloads' directory locally
    

        self.update_job_status(job_data['job_id'], changes={'status': 'downloading'})
        s3_key = os.path.join(scan_id, 'input', chunk_filename)
        s3_client = boto3.client('s3', aws_access_key_id=self.aws_access_key, aws_secret_access_key=self.aws_secret_key)
        s3_client.download_file("bucket_name", s3_key, input_file)
    
        output_dir = os.path.join('uploads', scan_id, 'output')
        output_file = os.path.join(output_dir, chunk_filename)
        with open(input_file, 'r') as f:
            content = f.read()
            self.update_job_status(job_data['job_id'], changes={'status': 'executing'})
            command = self.get_module_cmd(job_data['module'], input_file, output_file)
            print(command)
            subprocess.run("mkdir -p " + output_dir, shell=True)
            cmd = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            return_code = cmd.returncode

        if return_code != 0:
            print(f'Error executing command: {command}')
            print(f'Error: {cmd.stderr.decode("utf-8")}')
            self.update_job_status(job_data['job_id'], changes={'status': 'cmd failed'})
            return
        elif return_code == 0:
            print(f'Command executed successfully: {command}')
            self.update_job_status(job_data['job_id'], changes={'status': 'uploading'})

        # Upload output file to S3
        s3_key = os.path.join(job_data["scan_id"], 'output', chunk_filename)
        try:
            s3_client.upload_file(output_file, "bucket_name", s3_key)
            self.update_job_status(job_data['job_id'], changes={'status': 'complete'})
        except FileNotFoundError:
            print(f'File {output_file} not found')
            self.update_job_status(job_data['job_id'], changes={'status': 'upload failed - file not found'})
        except NoCredentialsError:
            print('Credentials not available')
            self.update_job_status(job_data['job_id'], changes={'status': 'upload failed - credentials'})
        except Exception as e:
            print(e)
            self.update_job_status(job_data['job_id'], changes={'status': 'upload failed - unknown'})

        


    def process_jobs(self):
        while True:
            try:
                job_data = self.get_job()

                if job_data:
                    self.process_chunk(job_data)
                else:
                    time.sleep(10)
            except exception as e:
                print(e)
                print('error getting job')
                time.sleep(10)
            time.sleep(0.8)


    
def main():
    parser = argparse.ArgumentParser(description='Worker for job processing')
    parser.add_argument('--server-url', required=True, help='Server URL')
    parser.add_argument('--api-key', required=True, help='API key')
    parser.add_argument('--worker-id', required=True, help='Worker ID')
    parser.add_argument('--aws-access-key', required=True, help='AWS Access Key ID')
    parser.add_argument('--aws-secret-key', required=True, help='AWS Secret Access Key')


    parser.add_argument('--max-jobs', required=False, default=1, type=int, help='Maximum number of jobs to process before exit')
    args = parser.parse_args()

    job_processor = JobProcessor(args.server_url, args.api_key, args.worker_id, args.max_jobs, args.aws_access_key, args.aws_secret_key)
    job_processor.process_jobs()

    threads = []
    for i in range(2):
        thread = threading.Thread(target=job_processor.process_jobs)
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()


if __name__ == '__main__':
    main()

