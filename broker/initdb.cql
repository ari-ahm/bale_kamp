CREATE KEYSPACE IF NOT EXISTS broker WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE broker;

CREATE TABLE IF NOT EXISTS messages(
    id UUID,
    body TEXT,
    subject TEXT,
    PRIMARY KEY ( id, subject )
);