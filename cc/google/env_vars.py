import os


# Environment variables, set locally for testing and when deployed to gcloud.
path_to_google_service_account = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
path_to_firebase_service_account = os.getenv('FIREBASE_SERVICE_ACCOUNT')

cloud_project_id = os.getenv('GCLOUD_PROJECT')
cloud_region = os.getenv('GCLOUD_REGION')
device_registry = os.getenv('GCLOUD_DEV_REG')

notifications_topic_subs = os.getenv('GCLOUD_NOTIFICATIONS_TOPIC_SUBS')
dev_reg = os.getenv('GCLOUD_DEV_REG')
dev_events = os.getenv('GCLOUD_DEV_EVENTS')

bq_dataset = os.getenv('BQ_DATASET')
bq_table = os.getenv('BQ_TABLE')
cs_bucket = os.getenv('CS_BUCKET')
cs_upload_bucket = os.getenv('CS_UPLOAD_BUCKET')



