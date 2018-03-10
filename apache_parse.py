#########################
## Test parsing log data into a dict with regex and pushing into kafka as a string
## I could just push the raw data and parse after but all for learning. parse both sides
##

import re
from collections import Counter
from confluent_kafka import Producer

def parse_apache(file):
  p = Producer({'bootstrap.servers': 'kafka-priv'})
  #Regex to support Access or Error logs. Separate or single file
  access = r'(?P<host>\S+)\s+\S+\s+(?P<user>\S+)\s+\[(?P<time>.+)\]\s+"(?P<request>.*)"\s+(?P<status>[0-9]+)\s+(?P<size>\S+)\s+"(?P<referrer>.*)"\s+"(?P<agent>.*)"\s*\Z'
  error = r'^\[(?P<time>.*?)\]\s\[(?P<level>.*?)\]\s\[pid (?P<pid>[^\]]*)\]\s(\[client (?P<client>[^\]]*)\]\s)?(?P<message>.*)$\s*\Z'
  access_re = re.compile(access)
  error_re = re.compile(error)
  log = open(file, 'r')
  logs_access = []
  logs_error = []
  logs_unmatched = []

  for line in log.readlines():
    if access_re.match(line):
      logs_access.append(access_re.match(line).groupdict())
    elif error_re.match(line):
      logs_error.append(error_re.match(line).groupdict())
    else:
      logs_unmatched.append(line)

  for line in logs_access:
    p.produce('httpd_logs_access', str(line).encode('utf-8'))
  p.flush()
   
parse_apache('/var/log/httpd/access_log')

