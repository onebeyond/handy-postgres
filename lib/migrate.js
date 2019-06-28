const R = require('ramda');
const path = require('path');
const Promise = require('bluebird');
const marv = Promise.promisifyAll(require('marv'));
const driver = require('marv-pg-driver');

module.exports = (logger) => (config) => {
  logger.info('Starting migration ...');

  const migrationConfigs = [].concat(config.migrations);
  const getOptions = R.assoc('connection', config);

  const migrate = (migrationConfig) => {
    const options = R.clone(getOptions(migrationConfig));
    options.connection.user = config.migrationsUser || config.user;
    options.connection.password = config.migrationsPassword || config.password;

    const migrationPath = path.resolve(migrationConfig.directory);

    return marv.scanAsync(migrationPath, options)
      .then((migrations) => marv.migrateAsync(migrations, driver(options)));
  };

  return R.reduce((result, migrationConfig) => result.then(() => migrate(migrationConfig)),
    Promise.resolve(),
    migrationConfigs);
};
