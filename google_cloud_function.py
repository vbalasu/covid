def gcs_object_created(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    file = event
    print(f"Processing file: {file['name']}.")
    import requests
    response = requests.post('https://8hw48j1041.execute-api.us-west-2.amazonaws.com/api/gcs_object_created', json={'key': file['name']})
    print(response.text)

