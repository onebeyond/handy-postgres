const R = require('ramda');
const Promise = require('bluebird');
const initPool = require('./lib/pool');
const { format } = require('sql-params-format');
const createRunner = require('./lib/runner');
const migrate = require('./lib/migrate');

const dotPath = R.useWith(R.path, [R.split('.')]);

const Pg = ({ pg = require('pg'), configPath = 'pg', logger = console }) => {
  let api = {};
  let pool;
  let queryRunner;

  const createPool = (config) => {
    if (!config)
      throw new Error('Unable to create connection pool, check your configuration.');
    return initPool(logger)(pg, config);
  };

  const start = (config, defaultPool) =>
    Promise.resolve()
      .then(() => {
        const pickedConfig = dotPath(configPath, config);
        const sqlPath = pickedConfig && pickedConfig.sql;
        const isolationLevel = pickedConfig && pickedConfig.isolationLevel;
        queryRunner = createRunner(sqlPath, format, isolationLevel);
        pool = defaultPool || createPool(pickedConfig);
        const promisifiedPool = Promise.promisifyAll(pool);
        pool = pool.getConnection ? pool : Object.assign(pool, { getConnection: () => promisifiedPool.connectAsync().disposer((client) => client.release()) });
        api = queryRunner(pool);
        return (pickedConfig && pickedConfig.migrations)
          ? Promise.using(pool.getConnection(), () => {}).then(() => migrate(logger)(pickedConfig))
          : Promise.resolve();
      })
      .then(() => api);

  const stop = () => {
    if (!pool) return Promise.resolve();
    return pool.end();
  };

  return {
    start,
    stop,
  };
};

module.exports = Pg;
