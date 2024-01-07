#!/usr/bin/env python3

import argparse
import random

# take input file as argument
parser = argparse.ArgumentParser()

parser.add_argument("input_file", help="input file containing subdomains")
parser.add_argument("instances", help="number of instances to run")

parser.parse_args()


# this script is to benchmark batches of job runs.
input_file = parser.parse_args().input_file


f = open(input_file, "r")
input_file = f.readlines()
f.close()

# shuffle input_file into variable
shuffled = input_file
random.shuffle(shuffled)


total_lines = len(input_file)
instances = int(parser.parse_args().instances)
batch_size = int(total_lines / instances) / 1.7 
sample_size = int(batch_size / 2)

if total_lines < instances:
    instances = total_lines
    batch_size = 1
    sample_size = 1
elif batch_size > 1000:
    sample_size = batch_size / 150
else:
    sample_size = batch_size / 7

magnification_factor = batch_size / sample_size

print("Total lines: " + str(total_lines))
print("Batch size: " + str(batch_size))
print("Sample size: " + str(sample_size))
print("Magnification factor: " + str(magnification_factor))

lines_to_get = int(sample_size * 13)
sample_lines = shuffled[:lines_to_get]

# write sample_lines to file
f = open("sample.txt", "w")
f.writelines(sample_lines)
f.close()


print("Sample written to sample.txt")





