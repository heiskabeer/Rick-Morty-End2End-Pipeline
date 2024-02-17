import json
import boto3
import io
import pandas as pd
import roman

def lambda_handler(event, context):

    s3_client = boto3.client('s3')
    
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']
    
    target_bucket = 'transformed-rick-morty'
    target_file_name = object_key[:4]
    target_file_name = f'{target_file_name}.csv'
    
    response = s3_client.get_object(Bucket = source_bucket, Key = object_key)

    data = response['Body'].read().decode('utf-8')
    # print(data)
    
    df = pd.read_csv(io.StringIO(data))
    
    # Transformations
    
    df.rename(columns={'no_of_episodes': 'no_of_episodes_appeared'}, inplace=True)
    
    # Add a suffix based on the count of occurrences within each group
    df['name'] = df['name'] + ' ' + df.groupby('name').cumcount().apply(lambda x: roman.toRoman(x+1) if x > 0 else '')

    print(df.head())

    csv_file = df.to_csv(index=False)

    #Load transformed CSV to transformed-rick-morty bucket
    s3_client.put_object(Bucket=target_bucket, Key=target_file_name, Body=csv_file) 
    
    
    return{
        'statusCode': 200,
        'body': json.dumps('CSV Transformed and loaded to new bucket!')
    }