import os
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import requests


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


def get_html_table_rows(page_content):
    soup = BeautifulSoup(page_content, "html.parser")
    table = soup.find(name='table', attrs={'class': 'sftable'})
    for br in table.find_all("br"):
        br.replace_with("\n" + br.text)  # note, </br> tags can technically contain text
    rows = table.findAll(lambda tag: tag.name == 'tr')
    return rows
