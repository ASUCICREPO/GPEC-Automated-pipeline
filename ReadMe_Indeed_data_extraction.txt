Indeed.com data extracter:

The code has been uploaded to GitHub: https://github.com/ASUCICREPO/GPEC-Automated-pipeline/blob/master/job-posting-data-extracter.py

Architecture:
————————————
We tried to execute the file in lambda, but the execution time of the job has exceeded more than 15 minutes. We tried splitting the job into 2 parts, and each half took more than 15 minutes. Since 15 minutes is the time limit on Lambda, we decided to execute the file in an EC2 instance.

Cron:
—————
Everyday, the job executes at 4:30 AM GMT, i.e., 9:30 PM Arizona time. To see/change the schedule of the job, login to ec2 instance and press: ‘crontab -e’. It will display the time at which the job is executed. Edit the file to change the time to run.

Job:
————
S3: https://s3.console.aws.amazon.com/s3/buckets/jobs-data-extract/?region=us-east-1

I’ve fetched all the jobs listed in the MSA regions and saved them in the file date_posted.csv. 

The date_posted.csv contains the city, state, no. of days back the job was posted and url link to the job.

Formula for calculating average number of jobs = (sum of number of days a closed was posted) / total number of jobs.

I’m saving the `sum of number of days a closed was posted` at city level in city_job_count.csv.

The city_job_count.csv contains each city name, sum of the days, total number of jobs that were closed for the city so far.

When the cron job runs, we fetch two files from S3(date_posted.csv and city_job_count.csv) and save them in the /tmp folder of the EC2 instance as date_posted_old.csv and city_job_count_old.csv. We read the file date_posted_old.csv and visit each record one-by-one. We go to each link and see if the job has been closed. If the job is closed, add the number of days the job was alive to the data present in city_job_count_old.csv and increase the total number of jobs by one for each job that got closed for the city the job was located in.

The new data is written to a file city_job_count_new.csv and the file is pushed to s3 as city_job_count.csv file. We aggregate the data at MSA level and create a new file called msa_job_count.csv and push the file to s3.


Finally, I go though all the data in indeed.com for each city and add newly created jobs to date_posted_new.csv. The file contains all the jobs from data_posted_old.csv, which were not closed. And all the open jobs, which we scanned through. The value for the number of days ago the job was posted is increased by a day for jobs that were not closed. Finally the file is pushed to S3 as date_posted.csv

The extracted data can be found on S3 bucket here:
https://s3.console.aws.amazon.com/s3/buckets/jobs-data-extract/?region=us-east-1

Quick Sight:
In quick sight, we’ve imported the msa_job_count file to the dashboard. We also set the refresh rate of the analysis to daily basis. So everyday once the file is reloaded from S3 and the dashboard gets updated.
