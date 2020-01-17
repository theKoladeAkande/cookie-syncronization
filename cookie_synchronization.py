from typing import Union, Callable
import redis
import time


class LimitExceededException(Exception):
    "custom exception"
    pass


MAX_RPS = 5 # requests per second
r = redis.StrictRedis(host='localhost', port=6381, db=0)



def save_sync(r: redis.StrictRedis, uid: str, partner_id: int, partner_uid: str):
    """Set the values for the pairs <partner_id, uid> and <partner_id, partner_uid>

    Do not forget to set the ttls which you defined in the function set_ttls
    
    Agrs:
        r (redis.StrictRedis): redis instance
        uid (str): cookie uid
        partner_id (int): id of the partner
        partner_uid (str): uid of the partner
  
    Examples:
        >>> save_sync(r, 'uid_1', 10, 'partner_uid_1')
    
    """
    partner_id2uid = str({partner_id:uid}) 
    partner_id2partner_uid = str({partner_id:partner_uid})
    
    r.hset('partner_id2uid',partner_id2uid, partner_uid)
    r.hset("partner_id2partner_uid", partner_id2partner_uid, uid)
    
    pipe = r.pipeline()
    
    if r.hget('partner_ttls', partner_id): 
            pipe.expire(partner_id2uid, r.hget('partner_ttls', partner_id).decode('utf-8'))
            pipe.expire(partner_id2partner_uid, r.hget('partner_ttls', partner_id).decode('utf-8'))
            pipe.execute()
    else: 
            pipe.expire(partner_id2uid, r.hget('partner_ttls', 0))
            pipe.expire(partner_id2partner_uid, r.hget('partner_ttls', 0))
            pipe.execute()


def limit_rate(r: redis.StrictRedis, function: Callable, partner_id: int):
    """Restrict function usage by MAX_RPS requests per second.
    
    If the amount of function calls is greater than MAX_RPS, raise LimitExceededException(f"{MAX_RPS} limit  is reached").
   
    Args:
        r (redis.StrictRedis): redis instance
        function (Callable): function from which limit_rate is called
        partner_id (int): id of the partner
       
    Examples:
        >>> limit_rate(r, get_partner_uid, partner_id)
        
    """
     
    current_time = int(time.time())

    r.set("hit:{1}:{2}:{3}".format(function.__name__,partner_id,time,current_time), 0)
       
    if int(r.get("hit:{1}:{2}:{3}".format(function.__name__,partner_id,time,current_time))\
        .decode('utf-8')) < MAX_RPS:

            r.incr("hit:{1}:{2}:{3}".format(function.__name__,partner_id,time,current_time))
     
    else:
          raise LimitExceededException('You have exceeded the MAX_RPS') 


def get_partner_uid(r: redis.StrictRedis, uid: str, partner_id: int):
    """Get the partner id by the pair (uid, partner id)
 
    Args:
        r (redis.StrictRedis): redis instance
        uid (str): cookie uid
        partner_id (int): id of the partner
      
    Examples:
        >>> get_partner_uid(r, 'e5a370cc-6bdc-43ae-baaa-8fd4531847f7', 12)

    """
    limit_rate(r, get_partner_uid, partner_id)
    partner_id2uid = str({partner_id:uid})
    return r.hget('partner_id2uid', partner_id2uid).decode('utf-8') or None


def get_uid(r: redis.StrictRedis, partner_id: int, partner_uid: str):
    """Get the uid by the pair (partner id, partner uid)

    Args:
        r (redis.StrictRedis): redis instance
        partner_id (int): id of partner
        partner_uid (str): uid of partner

    Examples:
        >>> get_uid(r, 12, '25b6e9a6-fca8-427c-94df-2577e62b2bf0')

    """
    limit_rate(r, get_uid, partner_id)

    partner_id2partner_uid = str({partner_id:partner_uid})
 
    return r.hget('partner_id2partner_uid', partner_id2partner_uid).decode('utf-8') or None


def set_ttls(r: redis.StrictRedis, ttls: dict):
    """Set the ttl by partner id

    Args:
        r (redis.StrictRedis): redis instance
        ttls (dict): dictionary of pairs <partner_id, ttl>

    Examples:
        >>> set_ttls(r, {12: 5, 3: 1})

    """
    for i in ttls.keys():
        r.hset('partner_ttls', i, ttls.get(i))
