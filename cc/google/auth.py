from cloud_common.cc.google import env_vars
from cloud_common.cc.google import datastore

def get_user_uuid_from_token(user_token):
    """Verifies session and returns user uuid"""

    session = datastore.get_one_from_DS(
        kind="UserSession", key="session_token", value=user_token
    )
    if not session:
        return None

    uuid = session.get("user_uuid")
    return uuid
