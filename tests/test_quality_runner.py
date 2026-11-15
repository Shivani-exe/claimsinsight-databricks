"""Offline tests of validation behavior, using small metadata and result fixtures."""

# Load the notebook and API adapter without Spark or Databricks credentials.
from decimal import Decimal
import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]

# Import source files directly because Databricks notebook filenames start with digits.
def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

dq = load_module('quality', ROOT / 'notebooks/05_Data_Quality_Tests.py')
api = load_module('live_runner', ROOT / 'scripts/run_live_tests.py')

# Build metadata fixtures from contracts, then mutate them to test failure paths.
def metadata_rows():
    return [{'table_name': table, 'column_name': column, 'full_data_type': datatype.upper()}
            for table, columns in dq.CONTRACTS.items() for column, datatype in columns.items()]

class QualityRunnerTests(unittest.TestCase):
    # Decimal cents beyond float precision must not collapse into equality.
    def test_decimal_precision(self):
        self.assertTrue(dq.equal_values('254560980.00', Decimal('254560980.00')))
        self.assertFalse(dq.equal_values('9007199254740992.01', Decimal('9007199254740992.02')))

    # Missing and malformed API values must not become zero or pass comparisons.
    def test_invalid_numeric_values_fail(self):
        for value in [None, '', 'not money', 'NaN']:
            with self.subTest(value=value):
                self.assertFalse(dq.equal_values(value, Decimal('0.00')))

    # The result recorder must retain failure evidence for the final assertion.
    def test_record_preserves_failed_measurement(self):
        rows = []
        dq.record(rows, 'net_payment', '99.99', Decimal('100.00'))
        self.assertEqual(rows[0]['status'], 'FAIL')
        self.assertEqual(rows[0]['measured'], '99.99')

    # A failed or empty summary must fail after presenting the available rows.
    def test_summary_failure_displays_before_raising(self):
        rows = []
        dq.record(rows, 'lost_claim', 1, 0)
        observed = []
        with self.assertRaises(AssertionError):
            dq.produce_summary(rows, lambda values: observed.extend(values))
        self.assertEqual(observed, rows)
        with self.assertRaises(AssertionError):
            dq.produce_summary([], lambda values: None)

    # A valid namespace is accepted, while SQL fragments are rejected before execution.
    def test_namespace_rejects_sql_fragments(self):
        self.assertEqual(len(dq.table_names('workspace.claimsinsight')), 8)
        for value in ['workspace.claimsinsight; DROP TABLE x', 'workspace', 'a.b.c', 'a-b.c']:
            with self.subTest(value=value), self.assertRaises(ValueError):
                dq.table_names(value)

    # A float payment column fails even when the numeric totals might look correct.
    def test_schema_rejects_nondecimal_payments(self):
        rows = metadata_rows()
        for row in rows:
            if row['table_name'] == 'silver_inpatient_claims_2008' and row['column_name'] == 'paid_amount':
                row['full_data_type'] = 'DOUBLE'
        results = []
        self.assertFalse(dq.check_schema(lambda sql: rows, results))
        failed = [row for row in results if row['status'] == 'FAIL']
        self.assertEqual(failed[0]['test_name'], 'schema.silver_inpatient_claims_2008.paid_amount')

    # Missing schema contracts stop data execution rather than generating misleading passes.
    def test_missing_schema_stops_data_queries(self):
        queries = []
        def execute(sql):
            queries.append(sql)
            return []
        results = dq.run_tests(execute)
        self.assertEqual(len(queries), 1)
        self.assertTrue(any(row['status'] == 'FAIL' for row in results))

    # Query exceptions must become failures while later independent checks continue.
    def test_query_error_is_reported_and_continues(self):
        queries = []
        def execute(sql):
            queries.append(sql)
            if 'information_schema.columns' in sql:
                return metadata_rows()
            raise RuntimeError('fixture query error')
        results = dq.run_tests(execute)
        failed = [row for row in results if row['status'] == 'FAIL']
        self.assertEqual(len(queries), 1 + len(dq.read_existing_tables()))
        self.assertEqual(len(failed), len(dq.read_existing_tables()))
        self.assertTrue(all('fixture query error' in row['measured'] for row in failed))

    # A one-cent mismatch in a returned aggregate must fail without suppressing other metrics.
    def test_one_cent_mismatch_is_detected(self):
        specs = {sql: expected for _, sql, expected in dq.read_existing_tables()}
        def execute(sql):
            if 'information_schema.columns' in sql:
                return metadata_rows()
            values = {key: str(value) for key, value in specs[sql].items()}
            if 'net_payment' in values:
                values['net_payment'] = '254560979.99'
            return [values]
        failed = [row for row in dq.run_tests(execute) if row['status'] == 'FAIL']
        self.assertEqual([row['test_name'] for row in failed], ['financial.silver.net_payment'])

    # Scalar queries returning no rows cannot pass silently.
    def test_empty_query_response_is_failure(self):
        def execute(sql):
            return metadata_rows() if 'information_schema.columns' in sql else []
        failed = [row for row in dq.run_tests(execute) if row['status'] == 'FAIL']
        self.assertEqual(len(failed), len(dq.read_existing_tables()))
        self.assertTrue(all('Expected one aggregate result row' in row['measured'] for row in failed))

    # API string decimals remain strings until the exact comparator evaluates them.
    def test_decode_keeps_decimal_strings(self):
        response = {'status': {'state': 'SUCCEEDED'},
                    'manifest': {'schema': {'columns': [{'name': 'payment'}]}},
                    'result': {'data_array': [['123.45']]}}
        self.assertEqual(api.decode_rows(response), [{'payment': '123.45'}])

    # Multiline SELECT queries must pass the read guard just as single-line queries do.
    def test_executor_accepts_newline_after_select(self):
        response = {'statement_id': 'fixture-id', 'status': {'state': 'SUCCEEDED'},
                    'manifest': {'schema': {'columns': [{'name': 'n'}]}},
                    'result': {'data_array': [['1']]}}
        ids = []
        with patch.object(api, 'cli_json', return_value=response) as cli:
            execute = api.make_executor('databricks', 'fixture', 'fixture', ids)
            self.assertEqual(execute('SELECT\n1 AS n'), [{'n': '1'}])
            self.assertEqual(ids, ['fixture-id'])
            self.assertEqual(cli.call_count, 1)
            with self.assertRaises(ValueError):
                execute('DELETE FROM fixture')

    # Truncated or partial API responses must never be interpreted as complete checks.
    def test_decode_rejects_failed_or_partial_results(self):
        fixtures = [
            {'status': {'state': 'FAILED', 'error': 'fixture failure'}},
            {'status': {'state': 'SUCCEEDED'}, 'manifest': {'truncated': True}},
            {'status': {'state': 'SUCCEEDED'}, 'result': {'next_chunk_internal_link': '/next'}}]
        for response in fixtures:
            with self.subTest(response=response), self.assertRaises(RuntimeError):
                api.decode_rows(response)

# Running this file directly is equivalent to unittest discovery.
if __name__ == '__main__':
    unittest.main()
