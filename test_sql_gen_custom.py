import unittest
from restructure_data import generate_sql, CHRDISTRICT_ENGLISH_CONST

# Mock constants if needed, but we are importing them.

class TestGenerateSQL(unittest.TestCase):
    def test_gaps_filled(self):
        # Mock processed data
        # Batch 1: Range 1-10 (small range for testing). 
        # Docs present: 1, 2, 5.
        # Missing: 3, 4, 6, 7, 8, 9, 10.
        
        processed_data = [
            {
                'sub_dir_id': 1,
                'doc_no': 1,
                'dist': 'TestDist',
                'sro': 'TestSRO',
                'year': '2023',
                'reg_type': 'Regular',
                'range_min': 1,
                'range_max': 10
            },
            {
                'sub_dir_id': 1,
                'doc_no': 2,
                'dist': 'TestDist',
                'sro': 'TestSRO',
                'year': '2023',
                'reg_type': 'eRegistration',
                'range_min': 1,
                'range_max': 10
            },
            {
                'sub_dir_id': 1,
                'doc_no': 5,
                'dist': 'TestDist',
                'sro': 'TestSRO',
                'year': '2023',
                'reg_type': 'iSarita_2.0',
                'range_min': 1,
                'range_max': 10
            }
        ]

        summary_rows, record_rows = generate_sql(processed_data)

        # Expect 1 summary row
        self.assertEqual(len(summary_rows), 1)
        
        # Expect 10 record rows (1 to 10)
        self.assertEqual(len(record_rows), 10)

        # Check content of record rows
        # Row 0 -> Doc 1 (Regular)
        self.assertIn("'Regular'", record_rows[0])
        self.assertIn("'Found'", record_rows[0])
        self.assertIn("1", record_rows[0]) # doc no

        # Row 2 -> Doc 3 (Missing)
        self.assertIn("'iSarita 2.0'", record_rows[2])
        self.assertIn("'Not Found'", record_rows[2])
        self.assertIn("3", record_rows[2]) # doc no

        # Row 4 -> Doc 5 (iSarita_2.0 -> iSarita 2.0 / Not Found)
        self.assertIn("'iSarita 2.0'", record_rows[4])
        self.assertIn("'Not Found'", record_rows[4])
        self.assertIn("5", record_rows[4])

        print("Test Passed: Gaps filled correctly.")

if __name__ == '__main__':
    unittest.main()
