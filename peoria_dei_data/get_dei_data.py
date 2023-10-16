import json
import os
import re
from urllib.parse import urlparse
import pandas as pd
import requests
import usaddress
from bs4 import BeautifulSoup


def cleanup_location(location):
    try:
        return usaddress.parse(location) if location else None
    except Exception as e:
        print(f'Error parsing location: {location}')


def cleanup_certifications(certification_information):
    all_certs = []
    certs = certification_information.split('\n')
    for cert in certs:
        cert_parts = cert.split(' - ', 1)
        if len(cert_parts) == 2:
            cert_location, cert_info = cert_parts
            cert_name, cert_expiration = cert_info.split(' expiring on ')
            cert_info = {
                "location": cert_location,
                "name": cert_name,
                "expiration": cert_expiration,
            }
            all_certs.append(cert_info)
    return all_certs


def cleanup_records(records):
    for r in records:
        r['certification_information'] = cleanup_certifications(r.get('certification_information', ''))
        r['location'] = cleanup_location(r.get('location', ''))


def parse_business_information(fieldset_node):
    bus_info = re.sub('\n{2,}', '\n', fieldset_node.text)
    bus_info_list = [x for x in bus_info.split('\n') if ':' in x]
    bus_info_dict = {}
    for item in bus_info_list:
        key, value = item.split(': ', 1)
        if not value or value == 'None':
            bus_info_dict[key] = None
        elif key in ['Email', 'Website']:
            bus_info_dict[key] = value.lower()
        elif key in ['Fax', 'Phone']:
            new_value = re.sub(r'[^0-9]', '', value)
            bus_info_dict[key] = new_value
        else:
            bus_info_dict[key] = value
    return bus_info_dict


def parse_table(rows):
    return_dict = {}
    for row in rows:
        fieldset_node = row.find('td').find('fieldset')
        legend_text = fieldset_node.find('legend').text
        if legend_text == 'Business Information':
            return_dict['business_information_raw'] = fieldset_node.text
            return_dict['business_information'] = parse_business_information(fieldset_node)
            return_dict['business_information_addl'] = fieldset_node.text[len(legend_text):]
            ownership_text_match = re.search('This is an? (.+)-Owned Business', fieldset_node.text)
            if ownership_text_match:
                ownership_text = ownership_text_match.group(1)
                return_dict['Ownership Text'] = ownership_text
                return_dict['Owner African American'] = 'African American' in ownership_text
                return_dict['Owner Hispanic'] = 'Hispanic' in ownership_text
                return_dict['Owner Asian'] = 'Asian' in ownership_text
                return_dict['Owner Veteran'] = 'Veteran' in ownership_text
                return_dict['Owner Female'] = 'Female' in ownership_text
        else:
            return_dict[legend_text] = fieldset_node.text[len(legend_text):]

    return_dict_flattened = {}
    for k, v in return_dict.items():
        if isinstance(v, dict):
            return_dict_flattened.update(v)
        else:
            return_dict_flattened[k] = v
    return return_dict_flattened


def get_html_table_rows(page_content):
    soup = BeautifulSoup(page_content, "html.parser")
    table = soup.find(name='table', attrs={'class': 'sftable'})
    for br in table.find_all("br"):
        br.replace_with("\n" + br.text)  # note, </br> tags can technically contain text
    rows = table.findAll(lambda tag: tag.name == 'tr')
    return rows


def process_url(url, local_page_dir):
    assert local_page_dir is not None, 'Must provide local_page_dir'
    assert os.path.isdir(local_page_dir), f'{local_page_dir} does not exist (need to manually create)'
    page_id = url.split('seq=')[-1]
    local_file = f'{local_page_dir}/{page_id}.html'

    if os.path.exists(local_file):
        print('Reading contents from local file')
        with open(local_file, 'r') as f:
            print(f'  Reading: {local_file}')
            url_contents = f.read()
    else:
        print('Reading contents from web')
        try:
            url_contents = requests.get(url).text
            print(f'Writing: {local_file}')
            with open(local_file, 'w') as f:
                f.write(url_contents)
        except Exception as e:
            print(f'Error getting data for {url}: {str(e)}')

    return {'url_contents': url_contents, 'page_id': page_id}


def read_url_list(peap_list_url):
    parsed_url = urlparse(peap_list_url)
    base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
    url_list = []

    try:
        peap_list_contents = requests.get(peap_list_url).text
        soup = BeautifulSoup(peap_list_contents, "html.parser")
        sftable_b = soup.find(name='table', attrs={'class': 'sftable_b'})
        rows = sftable_b.findAll(lambda tag: tag.name == 'tr')
        for index, row in enumerate(rows):
            if index == 0:
                continue
            first_cell = row.find('td')
            if first_cell is None:
                continue
            first_anchor = first_cell.find('a')
            if first_anchor is None:
                continue
            url_list.append(base_url + first_anchor.attrs['href'])
    except Exception as e:
        print(f'Error getting data from {peap_list_url}: {str(e)}')

    return url_list


def run_scrape(url_list_file_path, pages_dir_path):
    result_df_list = []
    url_list = read_url_list(url_list_file_path)
    for url in url_list:
        processed_url = process_url(url=url, local_page_dir=pages_dir_path)
        page_content = processed_url['url_contents']
        rows = get_html_table_rows(page_content)
        return_dict_flattened = parse_table(rows)
        return_dict_flattened['page_id'] = processed_url['page_id']
        return_dict_flattened['url'] = url
        result_df_list.append(pd.DataFrame(return_dict_flattened, index=[0]))

    return pd.concat(result_df_list)


if __name__ == '__main__':
    df = run_scrape(
        url_list_file_path='https://www.eprismsoft.com/business/showCert?id=140',
        pages_dir_path='../data/scrape_run_20231009/pages')

    output_csv_file_path = '../data/scrape_run_20231009/business_pages_parsed.csv'
    print(f'Saving CSV to {output_csv_file_path}')
    df.to_csv(output_csv_file_path, index=False)

    output_json_file_path = '../data/scrape_run_20231009/business_pages_parsed.json'
    print(f'Saving JSON to {output_json_file_path}')
    # exclude extraneous cols from the dataframe
    columns_to_exclude = ['business_information_raw', 'business_information_addl']
    filtered_df = df[df.columns.difference(columns_to_exclude)]
    # fix the column names to make suitable key names
    filtered_df.columns = filtered_df.columns.str.replace(' ', '_').str.lower()
    # output the dataframe as a JSON string
    json_str = filtered_df.to_json(orient='records')
    # parse the string as a JSON object
    parsed_json = json.loads(json_str)
    # reparse the certification and location information
    cleanup_records(parsed_json)
    # reparse the location information
    # pop keys with no value from the JSON object
    for item in parsed_json:
        keys_to_exclude = [key for key, value in item.items() if value is None]
        for key in keys_to_exclude:
            item.pop(key)
    # pretty print the JSON object, and write to the specified file path
    with open(output_json_file_path, 'w') as outfile:
        json.dump(parsed_json, outfile, indent=4)
