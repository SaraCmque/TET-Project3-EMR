import boto3
import json
import time

athena = boto3.client('athena')

def lambda_handler(event, context):
    print("Evento recibido:", json.dumps(event))
    
    path = event.get('path', '')
    print(f"Ruta solicitada: {path}")
    
    queries = {
        '/congestion-stats': "SELECT * FROM proyecto3_db.estadisticas_congestion",
        '/global-stats': "SELECT * FROM proyecto3_db.estadisticas_globales",
        '/correlations': "SELECT * FROM proyecto3_db.correlaciones",
        '/visualizations': "SELECT * FROM proyecto3_db.visualizaciones",
        '/test-data': "SELECT * FROM proyecto3_db.test_data",
        '/model-evaluation': "SELECT * FROM proyecto3_db.model_evaluation",
        '/predictions-gbt': "SELECT * FROM proyecto3_db.predictions_gbt",
        '/predictions-rf': "SELECT * FROM proyecto3_db.predictions_rf"
    }
    
    if path not in queries:
        print("Ruta inválida.")
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'Endpoint no válido. Rutas disponibles: /congestion-stats, /global-stats, /correlations, /visualizations'})
        }
    
    query_string = queries[path]
    print(f"Ejecutando consulta: {query_string}")
    
    try:
        query_execution = athena.start_query_execution(
            QueryString=query_string,
            QueryExecutionContext={'Database': 'proyecto3_db'},
            ResultConfiguration={
                'OutputLocation': 's3://eafit-project-3-bucket/athena-query-results/'
            }
        )
    except Exception as e:
        print(f"Error al iniciar ejecución de la consulta: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Error al iniciar la consulta Athena'})
        }

    execution_id = query_execution['QueryExecutionId']
    print(f"ID de ejecución de la consulta: {execution_id}")
    
    for i in range(30):
        status = athena.get_query_execution(QueryExecutionId=execution_id)
        state = status['QueryExecution']['Status']['State']
        print(f"Estado actual de la consulta (intento {i+1}): {state}")
        
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(1)
    
    if state != 'SUCCEEDED':
        print(f"Consulta no exitosa. Estado final: {state}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f'Error en la consulta Athena: {state}'})
        }

    print("Consulta completada exitosamente. Obteniendo resultados...")
    results = athena.get_query_results(QueryExecutionId=execution_id)

    columns = [col['Name'] for col in results['ResultSet']['ResultSetMetadata']['ColumnInfo']]
    print(f"Columnas obtenidas: {columns}")
    
    rows = []
    for row in results['ResultSet']['Rows'][1:]:
        parsed_row = {columns[i]: val.get('VarCharValue', '') for i, val in enumerate(row['Data'])}
        rows.append(parsed_row)
    
    print(f"Número de filas obtenidas: {len(rows)}")

    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
        'body': json.dumps({
            'data': rows,
            'query_execution_id': execution_id
        })
    }