from google.cloud import storage 


class CloudStorage:
    def __init__(
        self,
        project_id,
        bucket,
    ):
        self.client = storage.Client(project=project_id)
        self.bucket = bucket
