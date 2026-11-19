import json
from pathlib import Path
root=Path(__file__).resolve().parent
for item in json.loads((root/'provenance/text_parts.json').read_text()):
    with (root/'extracted_text'/item['file']).open('wb') as out:
        for name in item['parts']:out.write((root/'extracted_text'/name).read_bytes())
    print(item['file'])
