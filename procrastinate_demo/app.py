from typing import Type

import procrastinate

USE_ASYNC = True


import_paths = ["procrastinate_demo.tasks"]

connector_class: Type[procrastinate.BaseConnector]
if USE_ASYNC:
    from procrastinate.connector.aiopg import AiopgConnector

    connector_class = AiopgConnector
else:
    from procrastinate.connector.psycopg2 import Psycopg2Connector

    connector_class = Psycopg2Connector

app = procrastinate.App(connector=connector_class(), import_paths=import_paths)
app.open()
