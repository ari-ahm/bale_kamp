CREATE TABLE IF NOT EXISTS messages(
    id SERIAL PRIMARY KEY,
    body TEXT,
    subject TEXT NOT NULL,
    expiration TIMESTAMP NOT NULL
);

