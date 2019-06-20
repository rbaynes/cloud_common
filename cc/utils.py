import string
import random
from datetime import datetime, timezone

#------------------------------------------------------------------------------
def is_expired(expiration_date):
    """Returns whether something has expired
       Assumes that expiration_date is an 'aware' datetime object.
    """
    datenow = datetime.now(timezone.utc)
    return datenow > expiration_date


#------------------------------------------------------------------------------
def id_generator(size=6, chars=string.digits):
    return ''.join(random.choice(chars) for x in range(size))


#------------------------------------------------------------------------------
# Is the key is in the dict? if so return True.  if not False.
def key_in_dict(d, key):
    if key in d:
        return True
    return False


#------------------------------------------------------------------------------
def bytes_to_string(bs):
    if isinstance(bs, bytes):
        bs = bs.decode('utf-8')
    return bs



