#!/bin/bash
gcloud dataproc jobs submit pyspark --cluster=cluster-iz-dplr --region=us-east1 /home/ashfaqalamlearning/.git/spark/Usecase4_GcpGcsReadWritehive_cloud.py
if [ $? -ne 0 ]
then
echo "`date` error occured in the Pyspark job" &> /tmp/gcp_pyspark_schedule.log
else
echo "`date` Pyspark job is completed successfully" &> /tmp/gcp_pyspark_schedule.log
fi
echo "`date` gcloud hive ETL script is completed" &> /tmp/gcp_pyspark_schedule.log
