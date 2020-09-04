import en_core_web_sm
import requests
import pickle
from bs4 import BeautifulSoup
from ediblepickle import checkpoint
import os
import pandas as pd
import PyPDF2

nlp = en_core_web_sm.load()

page_links = []
for i in range(1999, 2013):
    p = 'https://www.sec.gov/divisions/enforce/friactions/friactions%s.shtml' %i
    page_links.append(p)


def change_link_name(link):
    temp = link.split("/")[-4:]
    test = ''
    for i in temp:
        test = test + '-' + i

    test = test.replace("-", "_")
    test = test.replace(":", "_")
    test = test.replace(".", "_")
    return (test)


cache_dir = 'cache'
if not os.path.exists(cache_dir):
    os.mkdir(cache_dir)


@checkpoint(key=lambda args, kwargs: change_link_name(args[0]) + '.p', work_dir=cache_dir)
def save_link(page_link):
    page = requests.get(page_link)
    return (page)


total = []
name, pdf_link, date, other = [], [], [], []

for row, page_link in enumerate(page_links):

    page = requests.get(page_link)
    soup = BeautifulSoup(page.text, "lxml")
    links = soup.select('body table tr td font table tr td')
    i = 0
    counter1 = counter2 = counter3 = counter4 = 0
    while i < len(links):
        temp_pdf_link = ''
        temp_name = ''
        temp_date = ''
        temp_other = ''
        if links[i].select('a'):
            try:
                if 'AAER' in links[i].select('a')[0].text:
                    try:
                        temp_pdf_link = 'https://www.sec.gov' + links[i].select('a')[0]['href']

                    except:
                        pass

                    try:
                        temp_name = links[i].select('a')[0].text

                    except:
                        pass

                    try:
                        i += 1
                        temp_date = links[i].text

                    except:
                        pass

                    try:
                        i += 1
                        temp_other = links[i].text

                    except:
                        pass

                    if temp_pdf_link and temp_name and temp_date and temp_other:
                        pdf_link.append(temp_pdf_link)
                        name.append(temp_name)
                        date.append(temp_date)
                        other.append(temp_other)
                        total.append((temp_pdf_link, temp_name, temp_date, temp_other))
            except:
                pass

        i += 1
    print(row, page_link, len(name), len(pdf_link), len(date), len(other), len(total))
    print('###############################################################################################')



df = pd.DataFrame()
df['name'] = name
df['link'] = pdf_link
df['date'] = date
df['other'] = other
df.head()

report = []
for i in range(len(df)):
    print(i)
    try:
        link = df['link'][i]
        page = requests.get(link)
        if page.status_code != 200:
            print('problem in page ', i)
            # print('############################################')

        if link[-3:] == 'pdf':
            temp = ''

            with open("my_pdf_%s.pdf" % i, 'wb') as my_data:
                my_data.write(page.content)

            open_pdf_file = open("my_pdf_%s.pdf" % i, 'rb')
            read_pdf = PyPDF2.PdfFileReader(open_pdf_file)

            num_pages = read_pdf.getNumPages()
            for j in range(num_pages):
                if read_pdf.isEncrypted:
                    read_pdf.decrypt("")
                    temp += read_pdf.getPage(j).extractText()

                else:
                    temp += read_pdf.getPage(j).extractText()

            report.append(temp)
        else:
            report.append(page.text)
    except:
        print('error in', i)
        print('############################')
        report.append('Error')


df['report'] = report

df.to_csv('scraped.csv', index=None)

company_names = pd.read_csv('Company_Names.csv')

company_dict = {}
for i in range(len(company_names)):
    print(i)
    company_name = company_names['conm'][i].lower()
    for j in range(len(df)):
        report = df['report'][j].lower()
        name = df['name'][j]
        date = df['date'][j].replace('\t', '').replace('\n', '').replace('\r', '').replace('  ', '')
        if company_name in report:
            if company_name in company_dict:
                company_dict[company_name].append((name, date))
            else:
                company_dict[company_name] = []
                company_dict[company_name].append((name, date))



f = open("company_year_CaseName.pkl","wb")
pickle.dump(company_dict,f)
f.close()

company_dict_count = {}
for key in company_dict:
    company_dict_count[key] = len(company_dict[key])

company_name_final = []
year_final = []
counter = []

df_1 = pd.DataFrame()

for key in company_dict:
    for value in company_dict[key]:
        temp_year = int(
            df['date'][df['name'] == value[0]].reset_index(drop=True)[0].replace('\t', '').replace('\n', '').replace(
                '\r', '').replace('  ', '')[-5:])
        company_name_final.append(key)
        year_final.append(temp_year)
        counter.append(1)

df_1['company'] = company_name_final
df_1['year'] = year_final
df_1['counter'] = counter
df_1.head()


df_2 = df_1.groupby(['company', 'year']).sum()
df_2 = df_2.reset_index()

temp_company = []
for i in range(len(company_names)):
    temp_company.append(company_names['conm'][i].lower())

company_names['company'] = temp_company


df_final = df_2.merge(company_names, left_on='company', right_on='company')
df_final.columns = ['company', 'year', 'num_reports', 'conm', 'gvkey', 'tic']

df_final = df_final[['conm', 'gvkey', 'tic', 'year', 'num_reports']]

df_final.to_csv('company_year_reports.csv', index=None)


