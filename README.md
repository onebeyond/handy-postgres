# handy-postgres

A handy API for Postgres which uses promises and [this super library](https://github.com/brianc/node-postgres), which also uses pool handling.

## Configuration

```js
"pg": {
  "user": "postgres",
  "database": "postgres",
  "password": "password",
  "host": "localhost",
  "port": 5432,
  "max": 10, // Maximum number of connections in the pool
  "sql": "myfolder/sql", // optional location for sql files
  "idleTimeoutMillis": 30000,
}
```

### Multiple Connections

You can specify a different config path by passing it in when instantiating the component:

```js
HandyPg({ configPath: 'mykey' });
```

You can find configuration examples [here](https://github.com/guidesmiths/handy-postgres/blob/master/test/e2e/config.test.js)

### Loading SQL Files

By specifying the location for sql files, it will automatically read and cache all of the sql in the folder for use in your model, reducing boilerplate.

File:

```sh
/src/model/sql/test.sql
```

How to use these shorthand functions is explained below.

## The API

After creating a Handy PG component, the following methods will be available:

| Property  | Description | Promise result |
| ------------- | ------------- | ----------- |
| withTransaction | Begin a new named transaction `pg.withTransaction(next)` | `(connection) => {}` |
| withConnection | Claims a connection from the pool `pg.withConnection(next)` |
| query | Execute an unformatted query using shorthands `SELECT $1::INT AS number` defaulting to a raw query if it cannot find one | `(result) => {}` |
| streamQuery | Same as `query`, but returns a stream ('data', 'error', 'end'). Multiple queries not possible (throws error). | `(stream) => {}` |
| formattedQuery | Execute a formatted query using shorthands `SELECT %L::INT AS %I` | `(result) => {}` |
| formattedStreamQuery | Same as formattedQuery but returns a stream ('data', 'error', 'end'). Multiple queries not possible (throws error). | `(stream) => {}` |
| insert | Insert data `(table, data, options)` (object `options` is optional. It admits the boolean property `_returning` to retrieve inserted data) | `() => {}` |
| update | Update data `(table, update, options)` (object `options` is optional. It admits where conditions and the `_returning` property as in `insert` )| `() => {}` |
| schema | Sets a schema and returns the query operations to use with that schema `(schema)`| `({ query, formattedQuery, insert, update }) => {}` |
| explain | Execute an explain plan for an unformatted query |
| formattedExplain | Execute an explain plan for a formatted query |
| copyFrom | Copy table contents from read stream |
| copyTo | Copy table contents to write stream |


You can find some examples for query, formattedQuery, insert and update [here](https://github.com/guidesmiths/handy-postgres/blob/master/test/e2e/query.test.js)

## Transactions

Transactions are made easier via a helper `withTransaction` block.  This helper takes a function that receives a 'transaction' object, returning a promise chain where all your operations will be placed. The 'transaction' object gives you the same 'query' helpers as explained above, reusing a single connection for all operations within the tx. The usual rollback, commit and begin operations are also exposed but they are abstracted away by the 'withTransaction' helper.


```js
pg.withTransaction((tx) =>
  Promise.all([
    tx.schema('myschema').insert('films', myFilm1),
    tx.schema('myschema').insert('films', myFilm2),
  ])
)
.catch((err) => {
  // Error occurred (but it still rolled back and closed connection)
})
```

You can find some transactions examples [here](https://github.com/guidesmiths/handy-postgres/blob/master/test/e2e/tx.test.js)

### Isolation Levels

Sometimes you will need to use a different transaction isolation level than the default one. You can [read more about this here](https://www.postgresql.org/docs/9.1/static/transaction-iso.html).

handy-postgres lets you specify your own in config:

```
{
  withSql: {
    ...
    isolationLevel: 'REPEATABLE READ',
    ...
  }
}
```
Also, you could override this configuration on specific transactions by passing the isolation level as second argument whenever you use the `withTransaction` operation:

```js
pg.withTransaction((tx) =>
  Promise.all([
    tx.schema('myschema').insert('films', myFilm1),
    tx.schema('myschema').insert('films', myFilm2),
  ]), 'SERIALIZABLE' // I want this transaction in particular to use the SERIALIZABLE isolation level
)
.catch((err) => {
  // Error occurred (but it still rolled back and closed connection)
})
```
## Migrations

Handy postgres uses [marv](https://github.com/guidesmiths/marv) to offer migration support. To use it, you need to specify marv options in migrations field. It will use handy-postgres configuration as connection options for marv.

```js
"pg": {
  // ...
  "migrations": [{ "directory": "src/migrations", "namespace": "test", "filter": "\\.sql$" }],
}
```

You can also specify a different migration user, e.g.

```js
"pg": {
  "migrationsUser": "marv",
  "migrationsPassword": "secret",
  "migrations": [{ "directory": "src/migrations", "namespace": "test", "filter": "\\.sql$" }],

```

## Streams
If you would like to query a very large data set, you may have to use a stream, here's how:

```js
Promise.resolve()
  .then(() => pg.streamQuery('SELECT loads FROM data'))
  .then((stream) => {
     return new Promise((resolve, reject) => {
       stream.on('data', (data) => {
         // do something with data...
       });
       stream.on('error', reject);
       stream.on('end', () => resolve({ result: /*...*/ }));
    });
  })
```
Also check out [promisepipe](https://www.npmjs.com/package/promisepipe) and [promise-streams](https://www.npmjs.com/package/promise-streams)
