#!/bin/bash

mode=""  # Initialize the mode variable
# mode="strict"

echo "Starting the JobRunner script."

echo "Running Job 1."
spark-submit --master yarn job_runner.py 1

# Check if the previous command (Job 1) was successful
if [ $? -eq 0 ]
then
  echo "Job 1 completed successfully."
else
  echo "Job 1 failed."
  if [ "$mode" == "strict" ]
  then
    exit 1  # Exit the script if Job 1 fails and mode is strict
  fi
fi

echo "Running Job 2."
spark-submit --master yarn job_runner.py 2

# Check if the previous command (Job 2) was successful
if [ $? -eq 0 ]
then
  echo "Job 2 completed successfully."
else
  echo "Job 2 failed."
  if [ "$mode" == "strict" ]
  then
    exit 1  # Exit the script if Job 2 fails and mode is strict
  fi
fi

echo "Finished running all jobs."
