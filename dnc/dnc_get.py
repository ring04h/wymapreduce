
from multiprocessing.dummy import Pool
import os
import requests

def exists(fileName):
	path = os.path.join('./emls/', fileName)
	return os.access(path, os.F_OK)

def save_file(path, data):
	with open(os.path.join('./emls/', path), 'w') as fd:
		fd.write(data)

def download_eml(emailid):
	path = '{}.eml'.format(emailid)
	if not exists(path):
		url = 'https://wikileaks.org/dnc-emails/get/{}'.format(emailid)
		resp = requests.get(url).content
		save_file(path, resp)

if __name__ == '__main__':
	pool = Pool(processes=10)
	pool.map(download_eml, range(1,22455)) # total 22455
	pool.close()
	pool.join()