import json
import re
import sys
from urllib import urlopen

url = urlopen('openstates.org/api/v1/committees/')

print url

