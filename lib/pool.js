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
  pool.on('connect', client => {
    /*
        pg maintains the order of queued requests. Since the connect event is synchronous
        we can be sure the following statements will be executed before any other queries
    */
    client.on('error', (err) => logger.error(`Client error: ${err.message}`));
    client.on('notice', (notice) => {
        switch (notice.severity) {
            case 'DEBUG': {
                logger.debug(notice.message)
                break;
            }
            case 'LOG':
            case 'INFO':
            case 'NOTICE': {
                logger.info(notice.message)
                break;
            }
            case 'WARNING': {
                logger.warn(notice.message)
                break;
            }
            case 'EXCEPTION':
            default: {
                logger.error(notice.message)
                break;
            }
        }
    });
  });
  pool.on('error', (err) => logger.error(`Pool error: ${err.message}`));

  return {
    query: (text, values) => pool.query(text, values),
    getConnection: () => pool.connect().disposer((client) => client.release()),
    end: () => pool.end(),
  };
};
