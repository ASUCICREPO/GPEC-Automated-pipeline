from bs4 import BeautifulSoup
import requests
import csv
import boto3
import logging
import traceback
import time
import os
from botocore.exceptions import ClientError

jobs_feed_url_prefix = "https://www.indeed.com/jobs?q="
jobs_feed_url_location_prefix = "&l="
city_state_mid_term = ",+"
jobs_feed_url_radius_suffix = "&radius=0"
jobs_feed_url_suffix = "&start="
indeed_dot_com_prefix = "https://www.indeed.com"
DEBUG = True
job_type = "Insurance"
city_job_count_file_name = 'city_job_count.csv'
msa_job_count_file_name = 'msa_job_count.csv'
date_posted_file_name = 'date_posted.csv'
# file_path_prefix = '/Users/vinrar/Desktop/CIC/job-data-extracter'
file_path_prefix = ''
# file_path_prefix is empty in lambda

bucket_name = "jobs-data-extract"

city_list = ["Phoenix", "Mesa", "Chandler", "San Antonio", "New Braunfels", "Philadelphia", "Camden", "Wilmington",
             "Denver", "Aurora", "Lakewood", "Boston", "Cambridge", "Newton", "Chicago", "Naperville", "Elgin",
             "New York", "Newark", "Jersey City", "Seattle", "Tacoma", "Bellevue", "Los Angeles", "Long Beach",
             "Anaheim", "San Francisco", "Oakland", "Berkeley", "Washington", "Arlington", "Alexandria"]

city_state_map = {'Phoenix': 'AZ', 'Mesa': 'AZ', 'Chandler': 'AZ', 'San Antonio': 'TX',
                  'New Braunfels': 'TX', 'Philadelphia': 'PA', 'Camden': 'NJ', 'Wilmington': 'DE',
                  'Denver': 'CO', 'Aurora': 'CO', 'Lakewood': 'CO', 'Boston': 'MA', 'Cambridge': 'MA',
                  'Newton': 'MA', 'Chicago': 'IL', 'Naperville': 'IL', 'Elgin': 'IL', 'New York': 'NY',
                  'Newark': 'NJ', 'Jersey City': 'NJ', 'Seattle': 'WA', 'Tacoma': 'WA', 'Bellevue': 'WA',
                  'Los Angeles': 'CA', 'Long Beach': 'CA', 'Anaheim': 'CA', 'San Francisco': 'CA',
                  'Oakland': 'CA', 'Berkeley': 'CA', 'Washington': 'DC', 'Arlington': 'VA',
                  'Alexandria': 'VA'}

city_msa_map = {'Phoenix': 'Greater Phoenix Area', 'Mesa': 'Greater Phoenix Area', 'Chandler': 'Greater Phoenix Area',
                'San Antonio': 'Greater San Antonio', 'New Braunfels': 'Greater San Antonio',
                'Philadelphia': 'Greater Philadelphia', 'Camden': 'Greater Philadelphia',
                'Wilmington': 'Greater Philadelphia', 'Denver': 'Greater Denver Area', 'Aurora': 'Greater Denver Area',
                'Lakewood': 'Greater Denver Area', 'Boston': 'Greater Boston', 'Cambridge': 'Greater Boston',
                'Newton': 'Greater Boston', 'Chicago': 'Greater Chicago Area', 'Naperville': 'Greater Chicago Area',
                'Elgin': 'Greater Chicago Area', 'New York': 'New York Metropolitan Area',
                'Newark': 'New York Metropolitan Area', 'Jersey City': 'New York Metropolitan Area',
                'Seattle': 'Greater Seattle', 'Tacoma': 'Greater Seattle', 'Bellevue': 'Greater Seattle',
                'Los Angeles': 'Greater LA Area', 'Long Beach': 'Greater LA Area', 'Anaheim': 'Greater LA Area',
                'San Francisco': 'San Francisco Bay Area', 'Oakland': 'San Francisco Bay Area',
                'Berkeley': 'San Francisco Bay Area', 'Washington': 'Washington Metropolitan Area',
                'Arlington': 'Washington Metropolitan Area', 'Alexandria': 'Washington Metropolitan Area'}


def printf(input_string):
    if DEBUG is True:
        print(input_string)


def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def indeed_url_maker(job_type, city, state, csv_writer):
    try:
        # First page always exists
        url = jobs_feed_url_prefix + job_type + jobs_feed_url_location_prefix + city + city_state_mid_term + state + jobs_feed_url_radius_suffix
        scan_next_page = True

        while scan_next_page is True:
            response = requests.get(url)
            printf(url)
            soup = BeautifulSoup(response.text, 'html.parser')
            posts = soup.findAll(class_='jobsearch-SerpJobCard')

            for post in posts:
                sjcl_result_set = post.findAll(class_='sjcl')
                if sjcl_result_set is not None:
                    for sjcl in sjcl_result_set:
                        job_location = sjcl.find(class_='location')
                        if city in job_location.contents[0]:

                            date_posted = post.findAll(class_='date')[0].contents[0]
                            date_str = date_posted.split(' ')
                            days = -1

                            if "Just" in date_str[0]:
                                days = 0
                            elif "Today" in date_str[0]:
                                days = 0
                            # elif '+' not in date_str[0]:
                            #     days = int(date_str[0])
                            # Only add new jobs that are posted. The above line is uncommented only when
                            # we need all the records

                            if days != -1:
                                job_url = post.find(class_='title')
                                job_url_a = job_url.find(class_='jobtitle')
                                job_url_href = str(job_url_a['href'])
                                # if not job_url_href.startswith('/pagead') :
                                job_url_href = indeed_dot_com_prefix + job_url_href
                                csv_writer.writerow([city, state, str(days), job_url_href])
                                # printf(city + "," + state + "," + str(days) + "," + job_url_href)

            # Check if next page exists in pagination and scan for next pages
            pagination_result_set = soup.findAll(class_='pagination')

            if not pagination_result_set:
                return False

            pagination = pagination_result_set[0]
            # span_classes = pagination.findAll(class_='pn')
            #
            # found_next = False
            # for span_class in span_classes:
            #     printf(span_class.contents)
            #     if "Next" in str(span_class.contents):
            #         found_next = True
            #         # Get the next page id

            paginated_a_classes = pagination.findAll('a')
            found_next = False

            for paginated_a_class in paginated_a_classes:
                if "Next" in str(paginated_a_class.contents):
                    found_next = True
                    url = indeed_dot_com_prefix + paginated_a_class['href']

            if found_next is False:
                scan_next_page = False

        return True

    except Exception as e:
        printf('Exception while parsing jobs in: ' + city)
        traceback.printf_exc()
        printf(e)
        return False


def check_if_url_is_still_open(url):
    try:
        response = requests.get(url, headers={'Accept-Encoding': 'identity'})
        soup = BeautifulSoup(response.text, 'html.parser')
        title = soup.title.string

        if "Page Not Found" in title:
            return False
    except Exception as e:
        print("Error while visiting url: " + url)
        print("Removing the URL and making the job closed")
        traceback.printf_exc()
        printf(e)
        return False

    return True


if __name__ == "__main__":
    start_time_for_uploading_to_s3 = time.time()

    old__msa_job_count_file_path = file_path_prefix + '/tmp/msa_job_count_old.csv'
    new_msa_job_count_file_path = file_path_prefix + '/tmp/msa_job_count_new.csv'

    old_job_count_file_path = file_path_prefix + '/tmp/city_job_count_old.csv'
    new_job_count_file_path = file_path_prefix + '/tmp/city_job_count_new.csv'

    old_file_path = file_path_prefix + '/tmp/date_posted_old_file_from_s3.csv'
    new_file_path = file_path_prefix + '/tmp/date_posted.csv'

    # 1. Download the date_posted.csv file from S3
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)

    # for my_bucket_object in bucket.objects.all():
    #     print(my_bucket_object)

    s3.Bucket(bucket_name).download_file(date_posted_file_name, old_file_path)
    s3.Bucket(bucket_name).download_file(city_job_count_file_name, old_job_count_file_path)
    # s3.Bucket(bucket_name).download_file(msa_job_count_file_name, old__msa_job_count_file_path)
    # s3.Bucket(bucket_name).download_file('date_posted.csv', 'date_posted_new.csv')
    # s3.download_file(bucket_name, 'date_posted.csv', os.path.basename(file_name))

    city_job_count_map = {}
    with open(old_job_count_file_path, mode='r') as job_count_new_file:
        count_reader = csv.reader(job_count_new_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for row in count_reader:
            city_job_count_map[row[0]] = [row[1], row[2]]


    with open(old_file_path, mode='r') as job_count_new_file:
        jobs_reader = csv.reader(job_count_new_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        with open(new_file_path, mode='w') as date_posted_new_file:
            job_writer = csv.writer(date_posted_new_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            # 2. Check if each page is still available i.e., job is not closed
            for row in jobs_reader:
                printf(row)
                url = row[3]
                is_page_open = check_if_url_is_still_open(url)
                if is_page_open:
                    days = int(row[2]) + 1
                    job_writer.writerow([row[0], row[1], str(days), row[3]])
                else:
                    printf("job closed")
                    jobs_data = city_job_count_map[row[0]]
                    jobs_data[0] = int(jobs_data[0]) + int(row[2])
                    jobs_data[1] = int(jobs_data[1]) + 1
                # close the job and increase the metric for that city
                # check if the url is still open, if open increase the days count by 1
                # and append to the date_posted_new.csv file.

            start_time_for_data_pull = time.time()
            for city in city_list:
                state = city_state_map[city]
                printf("Fetching jobs data of the city: " + city)
                is_valid_page = indeed_url_maker(job_type, city, state, job_writer)
                if is_valid_page is False:
                    printf('End of traversing jobs at: ' + city)

            end_time = time.time()
            print("Total time taken to pull data: " + str(end_time - start_time_for_data_pull) + " seconds.")

    msa_job_count_map = {}

    # Upload the updated values to S3
    with open(new_job_count_file_path, mode='w') as job_count_new_file:
        count_writer = csv.writer(job_count_new_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for city in city_job_count_map:
            city_data = city_job_count_map[city]
            msa = city_msa_map[city]
            if msa not in msa_job_count_map:
                msa_data = []
                msa_data.append(int(city_data[0]))
                msa_data.append(int(city_data[1]))
                msa_job_count_map[msa] = msa_data
            else:
                msa_data = msa_job_count_map[msa]
                msa_data[0] = int(city_data[0]) + int(msa_data[0])
                msa_data[1] = int(city_data[1]) + int(msa_data[1])
                msa_job_count_map[msa] = msa_data

            count_writer.writerow([city, city_data[0], city_data[1]])

    # Upload the updated values to S3
    with open(new_msa_job_count_file_path, mode='w') as msa_count_new_file:
        count_writer = csv.writer(msa_count_new_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for msa in msa_job_count_map:
            city_data = msa_job_count_map[msa]
            average_posting_close_time = (float(city_data[0])/float(city_data[1]))
            count_writer.writerow([msa, round(average_posting_close_time, 2)])


    # Jobs are extracted and saved in a csv file. Have to export it to S3.
    upload_file(new_file_path, bucket_name, 'date_posted.csv')
    upload_file(new_job_count_file_path, bucket_name, 'city_job_count.csv')
    upload_file(new_msa_job_count_file_path, bucket_name, 'msa_job_count.csv')

    end_time_for_uploading_to_s3 = time.time()
    print("Total time taken to complete the job: " + str(end_time_for_uploading_to_s3 - start_time_for_uploading_to_s3) + " seconds.")