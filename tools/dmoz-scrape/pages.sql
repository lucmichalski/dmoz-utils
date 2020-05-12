CREATE TABLE categorized_pages (
    id BIGSERIAL PRIMARY KEY,
    url TEXT NOT NULL UNIQUE,
    odp_title TEXT,
    odp_desc TEXT,
    odp_category TEXT,
    web_title TEXT,
    web_body TEXT
);
