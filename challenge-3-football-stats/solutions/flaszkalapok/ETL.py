import subprocess
import requests
import json
import time

args = json.load(open('data.json','r'))
print('na')
proc = subprocess.Popen([args['solution_python_executor'],
                         '%s/flask_prep.py' % args['solution_folder']])
print('eddig')

while True:
    try:
        requests.get('http://127.0.0.1:5112/started')
        print('még')
        time.sleep(5)
        print('jó')
        break
    except:
    	pass
    	print('vagy?')

print(proc.pid)
print('DONE')
