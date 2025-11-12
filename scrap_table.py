import requests
from bs4 import BeautifulSoup
import boto3
import uuid

def lambda_handler(event, context):
    url = "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados"

    response = requests.get(url, timeout=10)
    if response.status_code != 200:
        return {
            'statusCode': response.status_code,
            'body': 'Error al acceder a la página web'
        }

    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find('table')

    if not table:
        return {
            'statusCode': 404,
            'body': 'No se encontró la tabla en la página web'
        }

    headers = [header.text.strip() for header in table.find_all('th')]
    rows = []

    for row in table.find_all('tr')[1:]:
        cells = row.find_all('td')
        if len(cells) == len(headers):
            rows.append({headers[i]: cells[i].text.strip() for i in range(len(cells))})

    # Solo los 10 últimos
    rows = rows[:10]

    if not rows:
        return {
            'statusCode': 500,
            'body': 'No se encontraron datos de sismos'
        }

    # Guardar en DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table_db = dynamodb.Table('TablaWebScrapping')

    # Limpiar tabla antes de insertar nuevos
    scan = table_db.scan()
    with table_db.batch_writer() as batch:
        for each in scan.get('Items', []):
            batch.delete_item(Key={'id': each['id']})

    # Insertar nuevos
    for i, row in enumerate(rows, start=1):
        row['#'] = i
        row['id'] = str(uuid.uuid4())
        table_db.put_item(Item=row)

    return {
        'statusCode': 200,
        'body': rows
    }
