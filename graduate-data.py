from bs4 import BeautifulSoup
import requests
import csv
import time
import itertools
import os


city_list = ["Phoenix", "Mesa", "Chandler", "San Antonio", "New Braunfels", "Philadelphia", "Camden", "Wilmington",
             "Denver", "Aurora", "Lakewood", "Boston", "Cambridge", "Newton", "Chicago", "Naperville", "Elgin",
             "New York", "Newark", "Jersey City", "Seattle", "Tacoma", "Bellevue", "Los Angeles", "Long Beach",
             "Anaheim", "San Francisco", "Oakland", "Berkeley", "Washington", "Arlington", "Alexandria"]

city_set = {"Phoenix", "Mesa", "Chandler", "San Antonio", "New Braunfels", "Philadelphia", "Camden", "Wilmington",
             "Denver", "Aurora", "Lakewood", "Boston", "Cambridge", "Newton", "Chicago", "Naperville", "Elgin",
             "New York", "Newark", "Jersey City", "Seattle", "Tacoma", "Bellevue", "Los Angeles", "Long Beach",
             "Anaheim", "San Francisco", "Oakland", "Berkeley", "Washington", "Arlington", "Alexandria"}

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

nces_base_url = "https://nces.ed.gov/collegenavigator/"

fileName = "MSA_Finance_graduates_count.csv"


def visit_college_and_extract_data(college_url):
    pass


if __name__ == "__main__":
    start_time = time.time()
    total_number_of_seats = {}
    total_finance_seats = {}
    msa_total_grads = {}
    msa_finance_grads = {}
    for city in city_state_map:
        nces_url = "https://nces.ed.gov/collegenavigator/?q=city&s=state"
        state = city_state_map[city]
        total_number_of_seats[city] = 0
        total_finance_seats[city] = 0
        nces_url = nces_url.replace('state', state)
        nces_url = nces_url.replace('city', city)
        response = requests.get(nces_url)
        soup = BeautifulSoup(response.text, 'html.parser')
        colleges = itertools.chain(soup.findAll(class_='resultsY'), soup.findAll(class_='resultsW'))

        for college in colleges:
            college_url_postfix = college.find('a')['href']
            college_url = nces_base_url + college_url_postfix
            visit_college_and_extract_data(college_url)
            print(college_url)
            response = requests.get(college_url)
            soup = BeautifulSoup(response.text, 'html.parser')
            subrows = soup.findAll(class_="subrow")
            Grand_total_string = "Grand total"
            Finance_String = "Finance"

            for subrow in subrows:
                # print(subrow)
                if Grand_total_string in subrow.find('td'):
                    # print(subrow)
                    st = str(subrow)
                    st = st.replace("</td><td>-", ",")
                    st = st.replace("</td></tr>", "")
                    st = st.replace('<tr class="subrow"><td scope="row">', "")
                    st = st.replace('<sup>d</sup>', '')
                    values = st.split('</td><td>')
                    local_count = 0
                    for value in values:
                        value = value.replace(',', '')
                        local = 0
                        try:
                            local = int(value)
                        except ValueError:
                            print("Not an int: " + str(value))
                        local_count += local
                    total_number_of_seats[city] = total_number_of_seats[city] + local_count
                    print(local_count)

            indents = soup.findAll(class_="level1indent")

            for indent in indents:
                # print(indent)
                # if str(indent.find('td')).find(Finance_String) != -1:
                if Finance_String in str(indent.find('td')):
                    # print(indent)
                    st = str(indent)
                    st = st.replace("</td><td>-", ",")
                    st = st.replace("</td></tr>", "")
                    st = st.replace('<tr class="level1indent"><td scope="row">', "")
                    st = st.replace('<sup>d</sup>','')
                    values = st.split('</td><td>')
                    # values = values[1].split(',')
                    local_count = 0
                    for value in values:
                        value = value.replace(',', '')
                        local = 0
                        try:
                            local = int(value)
                        except ValueError:
                            print("Not an int: " + str(value))
                        local_count += local
                    total_finance_seats[city] = total_finance_seats[city] + local_count
                    print(local_count)

    for city in city_state_map:
        msa = city_msa_map[city]
        if msa not in msa_total_grads:
            msa_total_grads[msa] = 0
        if msa not in msa_finance_grads:
            msa_finance_grads[msa] = 0

        msa_total_grads[msa] = msa_total_grads[msa] + total_number_of_seats[city]
        msa_finance_grads[msa] = msa_finance_grads[msa] + total_finance_seats[city]

    with open(fileName, mode='w') as job_count_new_file:
        count_writer = csv.writer(job_count_new_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for msa in msa_total_grads:
            count_writer.writerow([msa,msa_finance_grads[msa], msa_total_grads[msa]])


    end_time = time.time()
    print("Total time taken: " + str(end_time - start_time))