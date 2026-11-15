"""Execute the notebook's read-only checks with an authenticated Databricks CLI."""

# Use only the standard library; the official CLI owns authentication and refresh.
import argparse
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import subprocess
import tempfile
import time

# Import notebook definitions without triggering its Spark-specific auto-run cell.
ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK = ROOT / 'notebooks' / '05_Data_Quality_Tests.py'
spec = importlib.util.spec_from_file_location('claimsinsight_quality', NOTEBOOK)
dq = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dq)

# Isolate CLI invocation so errors do not masquerade as empty successful responses.
def cli_json(cli, profile, arguments):
    command = [cli, *arguments, '--profile', profile, '--output', 'json']
    result = subprocess.run(command, capture_output=True, text=True, check=False)
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or 'Databricks CLI command failed')
    return json.loads(result.stdout)

# Decode only complete, small API results; fail on truncation instead of dropping rows.
def decode_rows(response):
    if response['status']['state'] != 'SUCCEEDED':
        raise RuntimeError(str(response['status']))
    if response.get('manifest', {}).get('truncated'):
        raise RuntimeError('SQL result was truncated')
    result = response.get('result', {})
    if result.get('next_chunk_internal_link'):
        raise RuntimeError('Expected one small result chunk')
    columns = [column['name'] for column in response['manifest']['schema']['columns']]
    return [dict(zip(columns, row)) for row in result.get('data_array', [])]

# Submit only the internally generated SELECT/WITH checks to the existing warehouse.
def make_executor(cli, profile, warehouse, statement_ids, timeout=300):
    def execute(sql):
        if not re.match(r'^(SELECT|WITH)\b', sql.lstrip(), re.IGNORECASE):
            raise ValueError('Only generated read queries are accepted')
        body = {'warehouse_id': warehouse, 'statement': sql, 'wait_timeout': '10s',
                'on_wait_timeout': 'CONTINUE', 'disposition': 'INLINE', 'format': 'JSON_ARRAY'}
        # Request files contain SQL only and are removed by TemporaryDirectory.
        with tempfile.TemporaryDirectory(prefix='claimsinsight-dq-') as temporary:
            request = Path(temporary) / 'statement.json'
            request.write_text(json.dumps(body), encoding='utf-8')
            data = cli_json(cli, profile, ['api', 'post', '/api/2.0/sql/statements',
                                         '--json', '@' + str(request)])
        statement_id = data['statement_id']
        statement_ids.append(statement_id)
        # Bound local polling and cancel a timed-out read to avoid abandoned queries.
        deadline = time.monotonic() + timeout
        while data['status']['state'] in ('PENDING', 'RUNNING'):
            if time.monotonic() >= deadline:
                cli_json(cli, profile, ['api', 'post', '/api/2.0/sql/statements/' + statement_id + '/cancel'])
                raise TimeoutError('Statement exceeded local timeout: ' + statement_id)
            time.sleep(2)
            data = cli_json(cli, profile, ['api', 'get', '/api/2.0/sql/statements/' + statement_id])
        return decode_rows(data)
    return execute

# Write per-check results and a compact execution manifest before propagating failures.
def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cli', default='databricks', help='Official CLI executable or full path')
    parser.add_argument('--profile', default='claimsinsight')
    parser.add_argument('--warehouse-id', required=True)
    parser.add_argument('--namespace', default='workspace.claimsinsight')
    parser.add_argument('--output', type=Path, default=Path('live_test_results.local.json'))
    args = parser.parse_args()
    statement_ids = []
    started = datetime.now(timezone.utc).isoformat()
    execute = make_executor(args.cli, args.profile, args.warehouse_id, statement_ids)
    results = dq.run_tests(execute, args.namespace)
    args.output.write_text(json.dumps(results, indent=2), encoding='utf-8')
    manifest = {'started_utc': started, 'finished_utc': datetime.now(timezone.utc).isoformat(),
                'execution': 'Databricks SQL Statement Execution API',
                'notebook_sha256': hashlib.sha256(NOTEBOOK.read_bytes()).hexdigest(),
                'statement_ids': statement_ids, 'checks': len(results),
                'passed': sum(row['status'] == 'PASS' for row in results)}
    args.output.with_suffix('.manifest.local.json').write_text(json.dumps(manifest, indent=2), encoding='utf-8')
    dq.produce_summary(results)

# Do not connect merely because this module is imported by offline tests.
if __name__ == '__main__':
    main()
