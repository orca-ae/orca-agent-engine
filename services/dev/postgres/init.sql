-- One DB per stateful service.
CREATE DATABASE registry;
CREATE DATABASE transcriptstore;
CREATE DATABASE filestore;
CREATE DATABASE memorystore;
GRANT ALL PRIVILEGES ON DATABASE registry TO orca;
GRANT ALL PRIVILEGES ON DATABASE transcriptstore TO orca;
GRANT ALL PRIVILEGES ON DATABASE filestore TO orca;
GRANT ALL PRIVILEGES ON DATABASE memorystore TO orca;
