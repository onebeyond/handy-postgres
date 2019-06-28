DO
$$
BEGIN
   IF NOT EXISTS (
      SELECT
      FROM   pg_catalog.pg_user
      WHERE  usename = 'migrations') THEN
      CREATE USER migrations PASSWORD 'secret';
   END IF;
END
$$
LANGUAGE plpgsql;

GRANT ALL ON DATABASE postgres TO migrations;

