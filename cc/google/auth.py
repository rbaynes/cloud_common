from .env_vars import *
from .datastore import get_one_from_DS

def get_user_uuid_from_token(user_token):
    """Verifies session and returns user uuid"""

    session = get_one_from_DS(
        kind="UserSession", key="session_token", value=user_token
    )
    if not session:
        return None

    uuid = session.get("user_uuid")
    return uuid
