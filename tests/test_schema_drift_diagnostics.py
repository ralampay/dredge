"""Actionable drift diagnostics without exposing source record values."""
import unittest

from dredge.core.exceptions import UserError
from dredge.formats.validation import validate_record


class SchemaDriftDiagnosticsTests(unittest.TestCase):
    def test_unexpected_field_is_identified_without_its_value(self):
        schema = {'type': 'struct', 'nullable': False, 'fields': [
            {'name': 'id', 'type': 'integer', 'nullable': False}]}
        with self.assertRaises(UserError) as error:
            validate_record({'id': 1, 'metadata_real_user_id': 'private-value'}, schema)
        message = str(error.exception)
        self.assertIn("unexpected fields: 'metadata_real_user_id'", message)
        self.assertIn('expand discovery sampling', message)
        self.assertNotIn('private-value', message)

    def test_nested_drift_has_path_and_bounded_field_list(self):
        schema = {'type': 'struct', 'nullable': False, 'fields': [
            {'name': 'body', 'type': 'struct', 'nullable': False, 'fields': []}]}
        with self.assertRaises(UserError) as error:
            validate_record({'body': {f'field_{i:02d}': 'private' for i in range(30)}}, schema)
        message = str(error.exception)
        self.assertIn('$/body', message)
        self.assertIn('30 total', message)
        self.assertNotIn('field_20', message)
        self.assertNotIn('private', message)
