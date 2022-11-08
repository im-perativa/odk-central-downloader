from dotenv import dotenv_values
from requests.auth import HTTPBasicAuth
from zipfile import ZipFile

import os
import pandas as pd
import requests
import shutil
import typer


app = typer.Typer()
config = dotenv_values('.env')
config['URL'] = config['URL'][:-1] if config['URL'].endswith('/') else config['URL']

@app.command()
def main(form_name: str):
    print(f'Downloading {form_name}')

    DATA_DIR = os.path.join('data', form_name)
    URL = f'{config["URL"]}/v1/projects/{config["PROJECT_ID"]}/forms/{form_name}/submissions.csv.zip?groupPaths=false'
    FILENAME = os.path.join('download', f'{form_name}.zip')

    res = requests.get(URL, stream=True, auth=HTTPBasicAuth(config['USERNAME'], config['PASSWORD']))
    if res.status_code == 200:
        with open(FILENAME, 'wb') as fh:
            res.raw.decode_content
            shutil.copyfileobj(res.raw, fh)

    with ZipFile(FILENAME, 'r') as zip_file:
        zip_file.extractall(path=DATA_DIR)

    submission_df = pd.read_csv(os.path.join(DATA_DIR, f'{form_name}.csv'))
    instance_ids = submission_df['instanceID'].to_list()

    images = os.listdir(os.path.join(DATA_DIR, 'media'))
    instance_images_cols = submission_df.columns[submission_df.isin(images).any()]

    if not os.path.exists(os.path.join(DATA_DIR, 'individual')):
        os.makedirs(os.path.join(DATA_DIR, 'individual'))
    extra_csvs = [file for file in os.listdir(DATA_DIR) if ('.csv' in file and file != f'{form_name}.csv')]

    # Create individual folder
    for instance_id in instance_ids: 
        print(f'instanceID: {instance_id}')

        instance_id_safe = instance_id.replace('uuid:', '')
        os.makedirs(os.path.join(DATA_DIR, 'individual', instance_id_safe), exist_ok=True)

        # Copy the data
        with pd.ExcelWriter(os.path.join(DATA_DIR, 'individual', instance_id_safe, f'{form_name}.xlsx'), mode='w') as writer:
    
            submission_df[submission_df['instanceID'] == instance_id].to_excel(writer, sheet_name=form_name, index=False)

            for extra in extra_csvs:
                sheet_name = extra.replace(f'{form_name}-', '').replace('.csv', '')[:31] # .xlsx limit sheet name is 31 characters
                extra_df = pd.read_csv(os.path.join(DATA_DIR, f'{extra}'))
                extra_df[extra_df['PARENT_KEY'] == instance_id].to_excel(writer, sheet_name=sheet_name, index=False)

        # Copy the image
        for col in instance_images_cols:
            try:
                shutil.copy(os.path.join(DATA_DIR, 'media', str(submission_df.loc[submission_df['instanceID'] == instance_id, col].values[0])), os.path.join(DATA_DIR, 'individual', instance_id_safe))
            except Exception as e:
                pass
        
if __name__ == '__main__':
    for folder in ['download', 'data']:
        if not os.path.exists(folder):
            os.makedirs(folder)
    app()
