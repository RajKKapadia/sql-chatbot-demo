import os
import json

from utils import get_table_names, get_column_names
import config

tables_name = get_table_names()

for tn in tables_name:
    print(f'Creating data definition for {tn}.')
    print('Creating empty data definitions.')
    data_definitions = {
        "tables": [
            {
                "name": tn,
                "description": "",
                "columns": []
            }
        ]
    }
    columns = get_column_names(tn)
    for column in columns:
        data_definitions['tables'][0]['columns'].append(
            {
                "name": str(column[0]),
                "type": str(column[1]),
                "description": ""
            }
        )
    print('Writing data definitions.')
    with open(os.path.join(config.DEFINITION_DIR, f'{tn}.json'), 'w') as file:
        file.write(json.dumps(data_definitions, indent=2))

print(f'Finished working on the data definitions.')
