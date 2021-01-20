#!/usr/bin/env python3

import sys
from components.data_processing import preprocess

input=sys.argv[1]
output=sys.argv[2]

print("Starting data cleaning...")
preprocess(input,output)
print("Completed data cleaning!")
