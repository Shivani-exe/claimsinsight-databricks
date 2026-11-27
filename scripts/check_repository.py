"""Offline source, documentation, configuration, and evidence consistency checks."""

# Use only the Python standard library; do not import or execute pipeline notebooks.
import hashlib
import json
from pathlib import Path
import re
from urllib.parse import unquote, urlsplit

# Anchor all checks to this checkout rather than the caller's current directory.
ROOT = Path(__file__).resolve().parents[1]

# Check source syntax without starting Spark, streaming queries, or Delta writes.
def check_python(files):
    paths = [path for path in files if path.suffix == '.py']
    for path in paths:
        compile(path.read_text(encoding='utf-8'), str(path), 'exec')
    return len(paths)

# Resolve local Markdown file links, excluding external links and same-page anchors.
def check_links(files):
    checked = 0
    for path in files:
        if path.suffix != '.md':
            continue
        for target in re.findall(r'\]\(([^)]+)\)', path.read_text(encoding='utf-8')):
            parsed = urlsplit(target.strip('<>'))
            if parsed.scheme or parsed.netloc or not parsed.path:
                continue
            resolved = (path.parent / unquote(parsed.path)).resolve()
            assert resolved.is_relative_to(ROOT), f'Link escapes checkout: {path}: {target}'
            assert resolved.exists(), f'Broken link: {path}: {target}'
            checked += 1
    return checked

# Confirm saved notebook source has not diverged from the inspected export manifest.
def check_exports():
    manifest = json.loads((ROOT / 'evidence/source_manifest.json').read_text(encoding='utf-8'))
    for item in manifest['notebooks']:
        content = (ROOT / item['repository_path']).read_text(encoding='utf-8')
        digest = hashlib.sha256(content.encode('utf-8')).hexdigest()
        assert digest == item['sha256_utf8_lf'], item['repository_path']
    assert len(manifest['notebooks']) == 6
    return len(manifest['notebooks'])

# Inspect the recorded deployed DAG and historical task results, including its real spelling.
def check_configuration():
    job = json.loads((ROOT / 'config/deployed_job.json').read_text(encoding='utf-8'))
    tasks = {task['task_key']: task for task in job['settings']['tasks']}
    order = ['ingest_claims', 'stage_events', 'classify_events', 'process_claims', 'ausit_claims']
    assert set(tasks) == set(order)
    assert job['settings']['max_concurrent_runs'] == 1
    for index, key in enumerate(order):
        dependencies = [item['task_key'] for item in tasks[key].get('depends_on', [])]
        assert dependencies == ([] if index == 0 else [order[index - 1]])
    run = json.loads((ROOT / 'evidence/job_run.json').read_text(encoding='utf-8'))
    assert run['state']['result_state'] == 'SUCCESS'
    assert {task['task_key'] for task in run['tasks']} == set(order)
    assert all(task['state']['result_state'] == 'SUCCESS' for task in run['tasks'])
    return len(tasks)

# Reject obvious credentials and raw/state files; this is a pattern scan, not a security audit.
def check_publishable(files):
    credential = re.compile(r'dapi[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,}|gh[pousr]_[A-Za-z0-9]{20,}|BEGIN (?:RSA |EC )?PRIVATE[ ]KEY')
    forbidden_suffixes = {'.csv', '.parquet', '.zip', '.key', '.pem'}
    for path in files:
        assert path.suffix not in forbidden_suffixes, f'Data/secret file: {path}'
        assert not ({'checkpoints', 'incoming', 'landing', 'schemas'} & set(path.relative_to(ROOT).parts)), path
        assert not credential.search(path.read_text(encoding='utf-8')), f'Credential-like content: {path}'

# Print one concise report; any failed assertion or syntax/JSON error exits unsuccessfully.
def main():
    files = [path for path in ROOT.rglob('*') if path.is_file()
             and '.git' not in path.parts and '__pycache__' not in path.parts]
    json_files = [path for path in files if path.suffix == '.json']
    for path in json_files:
        json.loads(path.read_text(encoding='utf-8'))
    check_publishable(files)
    print(json.dumps({'status': 'PASS', 'python_sources_compiled': check_python(files),
                      'relative_links_checked': check_links(files), 'json_files_parsed': len(json_files),
                      'notebook_exports_verified': check_exports(),
                      'deployed_task_dependencies_verified': check_configuration(),
                      'credential_pattern_and_data_file_scan': 'PASS',
                      'scope': 'Offline checks of source and recorded evidence; no live execution'}, indent=2))

# Execute checks only when explicitly invoked from the command line.
if __name__ == '__main__':
    main()
