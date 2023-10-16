from scraper import run_scrape
import json
from peoria_dei_data.data_processing_utils import cleanup_records

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
