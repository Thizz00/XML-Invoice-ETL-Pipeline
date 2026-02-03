import json
import shutil
import concurrent.futures
import os
from datetime import datetime
from pathlib import Path
from lxml import etree
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import PythonOperator

HOST_DATA_PATH = f"{os.environ['PROJECT_ROOT']}/data"
BASE = Path('/opt/spark/data')
XSD_PATH = Path('/opt/airflow/dags/invoice_definition.xsd')
READY = BASE / 'correctly_validated'

def _validate_single(f, schema, err_dir):
    try:
        schema.assertValid(etree.parse(str(f)))
        shutil.move(str(f), READY / f.name)
    except:
        shutil.move(str(f), err_dir / f.name)

def validate_xml_schema_func():
    err_dir = BASE / 'incorrectly_validated'
    for d in [err_dir, READY]: d.mkdir(parents=True, exist_ok=True)
    schema = etree.XMLSchema(etree.parse(str(XSD_PATH)))
    files = list((BASE / 'raw').glob('*.xml'))
    
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(lambda f: _validate_single(f, schema, err_dir), files)

def cleanup_func():
    audit_dir, manual_dir = BASE / 'audit_results', BASE / 'manual'
    arch_dir = BASE / 'archive' / datetime.now().strftime("%Y%m%d")
    for d in [arch_dir, manual_dir]: d.mkdir(parents=True, exist_ok=True)

    def _process_report(report):
        shutil.copy(report, arch_dir / f"audit_log_{report.name}")
        with open(report) as f:
            for line in f:
                item = json.loads(line)
                src = READY / item['file_name']
                if src.exists():
                    dst = arch_dir if item['status'] == 'success' else manual_dir
                    shutil.move(str(src), dst / item['file_name'])

    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(_process_report, list(audit_dir.glob('*.json')))
    
    if audit_dir.exists(): 
        os.system(f"chmod -R 777 {audit_dir}")
        shutil.rmtree(audit_dir, ignore_errors=True)

with DAG('invoice_pipeline', start_date=datetime(2026, 1, 27), schedule=None, catchup=False) as dag:
    
    v = PythonOperator(task_id='validate_schema', python_callable=validate_xml_schema_func)

    s = DockerOperator(
        task_id='run_spark',
        image='spark-job:latest',
        auto_remove=True,
        network_mode='etl',
        docker_url='unix://var/run/docker.sock',
        mount_tmp_dir=False, 
        environment={
        'POSTGRES_URL': 'jdbc:postgresql://postgres:5432/erp_db',
        'POSTGRES_USER': 'user',
        'POSTGRES_PASS': 'password'
    },
        mounts=[{
            'source': HOST_DATA_PATH, 
            'target': '/opt/spark/data',
            'type': 'bind'
        }]
    )

    c = PythonOperator(task_id='cleanup', python_callable=cleanup_func)

    v >> s >> c