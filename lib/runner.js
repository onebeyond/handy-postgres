const R = require('ramda');
const fs = require('fs');
const path = require('path');
const Promise = require('bluebird');
const { from, to } = require('pg-copy-streams');
const QueryStream = require('pg-query-stream');
const debug = require('debug')('handy-postgres:runner');
const createTx = require('./transaction');

module.exports = (locations = [], formatFn = require('pg-format'), isolationLevel) => {
  const SQL_EXT = '.sql';

  const join = R.curryN(2, path.join);

  const sqlFile = name => path.extname(name) === SQL_EXT;

  const readFile = R.partialRight(fs.readFileSync, [{ encoding: 'utf-8' }]);

  const shortName = name => path.basename(name, SQL_EXT);

  const nameToContent = (acc, name) => R.assoc(shortName(name), R.trim(readFile(name)), acc);

  const loadSql = (location) => {
    const files = fs.readdirSync(location);
    const load = R.pipe(
      R.filter(sqlFile),
      R.map(join(location)),
      R.reduce(nameToContent, {})
    );
    return load(files);
  };

  const loadAllSql = R.pipe(R.map(loadSql), R.mergeAll);

  const prefixExplain = (queries) => R.keys(queries).reduce((acc, name) => R.assoc(name, `EXPLAIN (ANALYZE, FORMAT JSON) ${queries[name]}`, acc), {});

  const sqlFormatter = (sql, values) => [R.apply(formatFn, [sql].concat(values)), []];

  const _prepare = (format, queries, queryName, values = []) => {
    const sql = queries[queryName];
    if (!sql) return [queryName, [].concat(values)]; // defaulting to raw query
    return format(sql, [].concat(values));
  };

  const _run = (format, pool, queries, streamResults = false) => (queryName, values = []) => {
    const prepared = _prepare(format, queries, queryName, values);
    debug(`Running ${prepared}`);

    if (streamResults) {
      return Promise.using(
        pool.getConnection(),
        (client) => Promise.try(() => client.query(new QueryStream(...prepared))));
    }

    return pool.query(...prepared);
  };

  const tableSql = (schema, table) => [schema, table].filter(Boolean).map(formatFn.ident).join('.');

  const _prepareUpdate = (queries, schema, table, update, where) => {
    const sqlZip = (separator, obj) => {
      const ks = R.keys(obj);
      const vs = R.values(obj);
      const aEqB = (aa, bb) => {
        const isMultiple = Array.isArray(bb);
        return [
          formatFn.ident(aa),
          isMultiple ? ' IN(' : '=',
          formatFn.literal(bb),
          isMultiple ? ')' : '',
        ].join('');
      };
      return R.zipWith(aEqB, ks, vs).join(separator);
    };

    const tableClause = tableSql(schema, table);
    const updateClause = sqlZip(',', update);
    const whereClause = R.when(Boolean, R.concat('WHERE '), sqlZip(' AND ', where));

    return _prepare(sqlFormatter, queries, '_update', [tableClause, updateClause, whereClause]);
  };

  const _prepareInsert = (queries, schema, table, insertions) => {
    const toInsert = [].concat(insertions);
    const keys = R.pipe(R.chain(R.keys), R.uniq)(toInsert);
    const cols = R.map(formatFn.ident, keys);
    const columnClause = R.join(',', cols);

    const escapeValue = (value) => {
      let escaped;
      if (value !== null && !Array.isArray(value) && value === Object(value)) {
        escaped = `'${formatFn.string(JSON.stringify(value))}'`;
      } else {
        escaped = formatFn.literal(value);
      }
      return escaped;
    };

    const vals = R.map(
      R.pipe(
        R.props(keys),
        R.map(escapeValue),
        R.join(',')
      )
    )(toInsert);

    const tableClause = tableSql(schema, table);
    const valueClause = R.join('),\n(', vals);
    const params = [tableClause, columnClause, valueClause];
    return _prepare(sqlFormatter, queries, '_insert', params);
  };

  const _insert = (pool, queries, schema) => (table, toInsert) => {
    if (R.isEmpty(toInsert)) return Promise.resolve();
    const prepared = _prepareInsert(queries, schema, table, toInsert);
    debug(`Inserting ${prepared}`);
    return pool.query(...prepared);
  };

  const _update = (pool, queries, schema) => (table, update, where) => {
    const prepared = _prepareUpdate(queries, schema, table, update, where);
    return pool.query(...prepared);
  };

  const _withTransaction = (pool, makeRunner, schema) => (fn, customIsolationLevel) =>
    Promise.using(pool.getConnection(), (client) => Promise.try(() => {
      const poolOfOne = {
        query: client.query.bind(client),
        getConnection: () => client,
        end: () => {},
      };
      const runner = makeRunner(poolOfOne).schema(schema);
      const txIsolationLevel = customIsolationLevel || isolationLevel;
      const tx = createTx(runner, txIsolationLevel);
      return tx.begin()
              .then(() => fn(tx)
                  .then((result) => tx.commit().then(() => result))
                  .catch((err) => tx.rollback().then(() => Promise.reject(err))));
    }));

  const _copyFrom = (pool, schema) => (readStream, table) =>
    Promise.using(pool.getConnection(), (client) => Promise.try(() => {
      const sql = formatFn('COPY %s FROM STDIN', tableSql(schema, table));
      const writeStream = client.query(from(sql));
      debug(`Running ${sql}`);
      return new Promise((resolve, reject) => {
        readStream.on('error', reject);
        readStream.pipe(writeStream).on('finish', resolve).on('error', reject);
      });
    }));

  const _copyTo = (pool, schema) => (writeStream, table) =>
    Promise.using(pool.getConnection(), (client) => Promise.try(() => {
      const sql = formatFn('COPY %s TO STDOUT', tableSql(schema, table));
      const readStream = client.query(to(sql));
      debug(`Running ${sql}`);
      return new Promise((resolve, reject) => {
        readStream.on('error', reject);
        readStream.pipe(writeStream).on('finish', resolve).on('error', reject);
      });
    }));

  const _queries = loadAllSql([path.join(__dirname, '..', 'sql')]);
  const queries = loadAllSql([].concat(locations));
  const explains = prefixExplain(queries);

  const _makeRunner = (pool) => {
    const api = (schema) => ({
      query: _run(R.unapply(R.identity), pool, queries),
      streamQuery: _run(R.unapply(R.identity), pool, queries, true),
      formattedQuery: _run(sqlFormatter, pool, queries),
      formattedStreamQuery: _run(sqlFormatter, pool, queries, true),
      explain: _run(R.unapply(R.identity), pool, explains),
      formattedExplain: _run(sqlFormatter, pool, explains),
      insert: _insert(pool, _queries, schema),
      update: _update(pool, _queries, schema),
      copyFrom: _copyFrom(pool, schema),
      copyTo: _copyTo(pool, schema),
      withTransaction: _withTransaction(pool, _makeRunner, schema),
      schema: api,
    });

    return api(null);
  };

  return _makeRunner;
};
