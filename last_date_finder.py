import json
from datetime import datetime, date
from pathlib import Path
def last_date():
    if Path('./last_date.json').exists():
        with open('./last_date.json') as file:
            di = json.load(file)
            return di["date"]
    else:
        return datetime.today().replace(year = datetime.today().year-1).replace(microsecond=0)
    
    
def create_last_date():
    di =  {"date": datetime.today().replace(microsecond=0).isoformat().replace('T', ' ')}
    with open('./last_date.json', 'w') as file:  
        json.dump(di, file)