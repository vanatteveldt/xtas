# Default configuration for Celery; can be overridden with an
# xtas_celeryconfig module in the PYTHONPATH.

import os
from kombu import Exchange, Queue


_BROKER_HOST=os.environ.get('XTAS_BROKER_HOST', '127.0.0.1')
_BROKER_USERNAME=os.environ.get('XTAS_BROKER_USERNAME', 'guest')
_BROKER_PASSWORD=os.environ.get('XTAS_BROKER_PASSWORD', 'guest')

BROKER_URL = 'amqp://{user}:{passwd}@{host}:{port}//'.format(
    user=_BROKER_USERNAME, passwd=_BROKER_PASSWORD, host=_BROKER_HOST, port=5672)
CELERY_RESULT_BACKEND = 'amqp'

CELERY_QNAME = os.environ.get('XTAS_CELERY_QUEUE', 'xtas')
CELERY_DEFAULT_QUEUE = CELERY_QNAME
CELERY_DEFAULT_EXCHANGE_TYPE = 'direct'

CELERY_QUEUES = (
    Queue(CELERY_QNAME, Exchange('default'), routing_key=CELERY_QNAME),
    Queue('background', Exchange('default'), routing_key='background'),
)

CELERY_ROUTES = {
        'xtas.tasks.single.corenlp': {
            'queue': 'corenlp',
        },
}

CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
CELERY_ACCEPT_CONTENT=['json']
CELERY_TIMEZONE = 'Europe/Amsterdam'
CELERY_ENABLE_UTC = True

CELERY_TASK_RESULT_EXPIRES = 3600

# Uncomment the following to make Celery tasks run locally (for debugging).
CELERY_ALWAYS_EAGER = os.environ.get('XTAS_EAGER', 'N').upper().startswith('Y')

