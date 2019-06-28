const R = require('ramda');
const Promise = require('bluebird');

module.exports = (logger) => (pg, config) => {
  const defaultConfig = {
    port: 5432,
    max: 10,
    idleTimeoutMillis: 30000,
    Promise,
  };
  const poolConfig = R.merge(defaultConfig, config);

  logger.info(`Creating postgres connection pool: user ${poolConfig.user}, database ${poolConfig.database}, pool size ${poolConfig.max}`);
  const pool = new pg.Pool(poolConfig);
  pool.on('error', (err) => logger.error(`Pool error: ${err.message}`));

  return {
    query: (text, values) => pool.query(text, values),
    getConnection: () => pool.connect().disposer((client) => client.release()),
    end: () => pool.end(),
  };
};
