CREATE TABLE test_schema.films  (
    code        char(5) CONSTRAINT firstkey PRIMARY KEY,
    title       varchar(1024) NOT NULL,
    "dateProd"  TIMESTAMPTZ,
    kind        varchar(10),
    len         integer NOT NULL
);
