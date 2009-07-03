"""
Twisted API for Amazon's various web services.
"""

from .s3 import AmazonS3
from .sdb import AmazonSDB
from .sdb import sdb_now, sdb_now_add, sdb_parse_time, sdb_latitude, sdb_longitude
from .sqs import AmazonSQS