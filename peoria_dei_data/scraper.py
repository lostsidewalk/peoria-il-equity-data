import re

import pandas as pd

from web_utils import read_url_list, process_url, get_html_table_rows


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
