from chalicelib import cross_cloud
from chalice import Chalice
import boto3, json
app = Chalice(app_name='covid')
queue_url = 'https://sqs.us-west-2.amazonaws.com/163305015547/dataexchange-publish'

@app.route('/')
def index():
    return {'hello': 'world'}

@app.route('/publish_to_dx/{json_config}')
def publish_to_dx(json_config):
    print('Reading config file ', json_config)
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket='trifacta-covid-trifactabucket-q1itzd5kh96', Key=json_config)
    config = json.load(obj['Body'])
    print(config)
    sqs = boto3.client('sqs', region_name='us-west-2')
    sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(config))
    # s3.put_object(Body=json.dumps(config), Bucket='trifacta-covid-trifactabucket-q1itzd5kh96', Key='dx_job.json')
    print('Added config to queue: dataexchange-publish')
    return config

@app.on_sqs_message(queue='dataexchange-publish', batch_size=1)
def handle_sqs_message(event):
    for record in event:
        config = json.loads(record.body)
        print(f'Received config: {config}')
        from chalicelib import dataexchange
        dataexchange.publish_to_dx(config)

@app.on_s3_event(bucket='trifacta-covid-trifactabucket-q1itzd5kh96', events=['s3:ObjectCreated:*'])
def handle_object_created(event):
    print('Processing event for ', event.key)
    # if event.key == 'dx_job.json':
    #     print('Reading config file ', event.key)
    #     s3 = boto3.client('s3')
    #     obj = s3.get_object(Bucket='trifacta-covid-trifactabucket-q1itzd5kh96', Key=event.key)
    #     config = json.load(obj['Body'])
    #     from chalicelib import dataexchange
    #     dataexchange.publish_to_dx(config)
    # else:
    target_bucket = 'from-aws-trifacta-covid-trifactabucket-q1itzd5kh96'
    print('s3 to gcs', event.bucket, target_bucket, event.key)
    cross_cloud.s3_to_gcs(event.bucket, target_bucket, event.key)

# http POST https://8hw48j1041.execute-api.us-west-2.amazonaws.com/api/gcs_object_created key=un_raw/CASE_DATA/UN_Cases_04142020.csv
# response = requests.post('https://8hw48j1041.execute-api.us-west-2.amazonaws.com/api/gcs_object_created', json={'key': 'un_raw/CASE_DATA/UN_Cases_04142020.csv'})
@app.route('/gcs_object_created', methods=['POST'])
def gcs_object_created():
    key = app.current_request.json_body['key']
    print('covid_data_raw', 'from-gcs-covid-data-raw', key)
    cross_cloud.gcs_to_s3('covid_data_raw', 'from-gcs-covid-data-raw', key)
    return app.current_request.json_body

# The view function above will return {"hello": "world"}
# whenever you make an HTTP GET request to '/'.
#
# Here are a few more examples:
#
# @app.route('/hello/{name}')
# def hello_name(name):
#    # '/hello/james' -> {"hello": "james"}
#    return {'hello': name}
#
# @app.route('/users', methods=['POST'])
# def create_user():
#     # This is the JSON body the user sent in their POST request.
#     user_as_json = app.current_request.json_body
#     # We'll echo the json body back to the user in a 'user' key.
#     return {'user': user_as_json}
#
# See the README documentation for more examples.
#
