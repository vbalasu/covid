from chalicelib import cross_cloud
from chalice import Chalice
app = Chalice(app_name='covid')

@app.route('/')
def index():
    return {'hello': 'world'}

@app.on_s3_event(bucket='trifacta-covid-trifactabucket-q1itzd5kh96', events=['s3:ObjectCreated:*'])
def handle_object_created(event):
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
