import os
ES_HOST=os.environ.get("XTAS_ES_HOST", 'localhost')
ES_PORT=int(os.environ.get("XTAS_ES_PORT", 9200))
