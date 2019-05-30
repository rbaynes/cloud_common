import os

# Environment variables, set locally for testing and when deployed to gcloud.
path_to_google_service_account = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
path_to_firebase_service_account = os.getenv('FIREBASE_SERVICE_ACCOUNT')
cloud_project_id = os.getenv('GCLOUD_PROJECT')
cloud_region = os.getenv('GCLOUD_REGION')
device_registry = os.getenv('GCLOUD_DEV_REG')



