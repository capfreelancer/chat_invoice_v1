import boto3
from botocore.exceptions import ClientError

from pprint import pprint
import json

s3 = boto3.client('s3')
S3_BUCKET_NAME = "freelancecap"

def get_rows_columns_map(table_result, blocks_map):
    rows = {}
    scores = []
    for relationship in table_result['Relationships']:
        if relationship['Type'] == 'CHILD':
            for child_id in relationship['Ids']:
                cell = blocks_map[child_id]
                if cell['BlockType'] == 'CELL':
                    row_index = cell['RowIndex']
                    col_index = cell['ColumnIndex']
                    if row_index not in rows:
                        rows[row_index] = {}
                    
                    scores.append(str(cell['Confidence']))
                    rows[row_index][col_index] = get_text(cell, blocks_map)

    return rows, scores

def get_text(result, blocks_map):
    text = ""
    if 'Relationships' in result:
        for relationship in result['Relationships']:
            if relationship['Type'] == 'CHILD':
                for child_id in relationship['Ids']:
                    word = blocks_map[child_id]
                    if word['BlockType'] == 'WORD':
                        if "," in word['Text'] and word['Text'].replace(",","").isnumeric():
                            text += '"' + word['Text'] + '"' + ' '
                        else:
                            text += word['Text'] + ' '
                    if word['BlockType'] == 'SELECTION_ELEMENT':
                        if word['SelectionStatus'] == 'SELECTED':
                            text += 'X '
    return text

def get_table_csv_results(file_name):
    with open(file_name, 'rb') as  file:
        img_test = file.read()
        bytes_array = bytearray(img_test)
        print('Image loaded', file_name)

    session = boto3.Session()
    client = session.client('textract')
    response = client.analyze_document(Document={'Bytes': bytes_array}, FeatureTypes=['TABLES'])

    blocks = response['Blocks']
    pprint(blocks)

    blocks_map = {}
    table_blocks = []
    for block in blocks:
        blocks_map[block['Id']] = block
        if block['BlockType'] == 'TABLE':
            table_blocks.append(block)

    if len(table_blocks) <= 0:
        return "<b> NO Table FOUND </b>"
    
    csv = ''
    for index, table in enumerate(table_blocks):
        csv += generate_table_csv(table, blocks_map, index + 1)
        csv += '\n\n'
    
    return csv

def generate_table_csv(table_result, blocks_map, table_index):
    rows, scores = get_rows_columns_map(table_result, blocks_map)

    table_id = 'Table_' + str(table_index)
    csv = 'Table: {0}\n\n'.format(table_id)

    for row_index, cols in rows.items():
        for col_index, text in cols.items():
            col_indices = len(cols.items())
            csv += '{}'.format(text) + ","
        csv += '\n'

    csv += "\n\n\n"
    return csv

def store_objectIn_s3(obj, object_name):
    if obj:
        s3.upload_file(obj, S3_BUCKET_NAME, Key=object_name)
        return "File Uploaded"
    else:
        return "Error Uploading S3 Object"
    
def get_signed_s3_Object(obj_nmae, expiration=3600):
    try:
        # res = s3.get_object(Bucket=S3_BUCKET_NAME, Key=obj_nmae)
        url = f'https://{S3_BUCKET_NAME}.s3.amazonaws.com/{obj_nmae}'
        # res_json = json.loads(res['Body'].read().decode('utf-8'))
    except ClientError as ce:
        return ce
    
    return url