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

  const start = (config, next) =>
    Promise.resolve()
      .then(() => {
        const pickedConfig = dotPath(configPath, config);
        const sqlPath = pickedConfig && pickedConfig.sql;
        const isolationLevel = pickedConfig && pickedConfig.isolationLevel;
        queryRunner = createRunner(sqlPath, format, isolationLevel);
        pool = createPool(pickedConfig);
        api = queryRunner(pool);
        return Promise.using(pool.getConnection(), () => {})
          .then(() => {
            if (pickedConfig.migrations) {
              return migrate(logger)(pickedConfig);
            }
            return null;
          });
      })
      .then(() => next(null, api))
      .catch(next);

  const stop = (next) => {
    if (!pool) return next();
    return pool.end()
    .then(next)
    .catch(next);
  };

  return {
    dependsOn: ['config'],
    start,
    stop,
  };
};

module.exports = Pg;
