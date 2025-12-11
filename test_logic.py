import unittest
from restructure_data import parse_folder_name, generate_sql, verify_batches

class TestRestructureLogic(unittest.TestCase):
    def test_parse_folder_name_regular(self):
        folder_name = "Regular_मुंबई_जिल्हा_Joint_S.R._Mumbai_1_(Mumbai_City_1_(Fort))_2009_2"
        expected = {
            "original_name": folder_name,
            "reg_type": "Regular",
            "dist": "मुंबई_जिल्हा",
            "sro": "Joint_S.R._Mumbai_1_(Mumbai_City_1_(Fort))",
            "year": "2009",
            "doc_no": 2
        }
        result = parse_folder_name(folder_name)
        self.assertEqual(result, expected)

    def test_value_based_batching_logic(self):
        items = [
            {"dist": "D1", "sro": "S1", "year": "2024", "doc_no": 1, "original_name": "Doc1"},
            {"dist": "D1", "sro": "S1", "year": "2024", "doc_no": 2001, "original_name": "Doc2001"}
        ]
        
        BATCH_SIZE = 2000
        global_batch_id = 0
        batch_map = {}
        processed_items = []
        key = ("D1", "S1", "2024")
        
        for item in items:
            doc_no = item['doc_no']
            logical_batch_index = (doc_no - 1) // BATCH_SIZE
            map_key = (key, logical_batch_index)
            
            if map_key not in batch_map:
                global_batch_id += 1
                batch_map[map_key] = global_batch_id
            
            item['sub_dir_id'] = batch_map[map_key]
            processed_items.append(item)
            
        self.assertEqual(processed_items[0]['sub_dir_id'], 1)
        self.assertEqual(processed_items[1]['sub_dir_id'], 2)
        
    def test_verify_batches(self):
        # Batch 1: Has Regular and eRegistration
        # Batch 2: Has ONLY eRegistration
        # Batch 3: Has iSarita
        data = [
            {'sub_dir_id': 1, 'reg_type': 'Regular'},
            {'sub_dir_id': 1, 'reg_type': 'eRegistration'},
            {'sub_dir_id': 2, 'reg_type': 'eRegistration'},
            {'sub_dir_id': 3, 'reg_type': 'iSarita_2.0'},
            {'sub_dir_id': 3, 'reg_type': 'Regular'}
        ]
        
        only_ereg, isarita = verify_batches(data)
        
        self.assertEqual(only_ereg, [2])
        self.assertEqual(isarita, [3])

if __name__ == '__main__':
    unittest.main()
