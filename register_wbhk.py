import requests
import json
import os

webhook_url = os.getenv('WEB_HK_API_ENDP')

reg_url = os.getenv('WEB_HK_REG_URL')

payload = json.dumps({"url": webhook_url})

headers = {'Content-Type': 'application/json'}

response = requests.post(reg_url, headers=headers, data=payload) 

print(response.text) 