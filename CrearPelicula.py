import json
import os
import uuid

import boto3


def log_json(tipo, datos):
    print(json.dumps({
        "tipo": tipo,
        "log_datos": datos
    }))


def lambda_handler(event, context):
    try:
        # Entrada (json)
        log_json("INFO", {"evento": event})
        tenant_id = event['body']['tenant_id']
        pelicula_datos = event['body']['pelicula_datos']
        nombre_tabla = os.environ["TABLE_NAME"]
        # Proceso
        uuidv4 = str(uuid.uuid4())
        pelicula = {
            'tenant_id': tenant_id,
            'uuid': uuidv4,
            'pelicula_datos': pelicula_datos
        }
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(nombre_tabla)
        response = table.put_item(Item=pelicula)
        # Salida (json)
        log_json("INFO", {"pelicula": pelicula, "dynamodb_response": response})
        return {
            'statusCode': 200,
            'pelicula': pelicula,
            'response': response
        }
    except Exception as error:
        log_json("ERROR", {
            "mensaje": str(error),
            "evento": event
        })
        raise
