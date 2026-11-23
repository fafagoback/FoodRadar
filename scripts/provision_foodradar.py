"""Create FoodRadar's independent DB and store scoped tokens in GitHub secrets.

No platform/write credentials are emitted or saved to the repository.
"""
import argparse
import os
import subprocess
from pathlib import Path
import requests


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--organization',required=True)
    parser.add_argument('--env-file',type=Path)
    parser.add_argument('--repo',default='fafagoback/FoodRadar')
    args = parser.parse_args()
    token = os.environ.get('TURSO_PLATFORM_TOKEN')
    if not token and args.env_file:
        for line in args.env_file.read_text(encoding='utf-8-sig').splitlines():
            if line.strip().startswith('TURSO_PLATFORM_TOKEN='):
                token = line.split('=',1)[1].strip().strip('\"\'')
    if not token:
        raise ValueError('TURSO_PLATFORM_TOKEN is required')
    session = requests.Session()
    session.headers['Authorization'] = 'Bearer '+token
    base = f'https://api.turso.tech/v1/organizations/{args.organization}'
    def call(method,path,**kwargs):
        response = session.request(method,base+path,timeout=30,**kwargs)
        if not response.ok:
            raise RuntimeError(f'Turso platform {method} {path}: HTTP {response.status_code}')
        return response.json()
    name = 'foodradar-packed-v1'
    existing = call('GET','/databases')['databases']
    db = next((d for d in existing if d['Name']==name),None)
    if db is None:
        db = call('POST','/databases',json={'name':name,'group':'default'})['database']
    url = 'https://'+db['Hostname']
    for auth,secret in [('full-access','FOODRADAR_TURSO_WRITE_TOKEN'),('read-only','FOODRADAR_TURSO_READONLY_TOKEN')]:
        scoped = call('POST',f'/databases/{name}/auth/tokens',params={'authorization':auth,'expiration':'never'})['jwt']
        subprocess.run(['gh','secret','set',secret,'--repo',args.repo],input=scoped,text=True,check=True)
    subprocess.run(['gh','secret','set','FOODRADAR_TURSO_DATABASE_URL','--repo',args.repo],input=url,text=True,check=True)
    print(f'Independent database ready: {name}; GitHub secrets configured for {args.repo}')


if __name__ == '__main__':
    main()
