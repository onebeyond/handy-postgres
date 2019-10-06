const debug = require('debug')('handy-postgres:tx');

module.exports = (runner, isolationLevel) => {
  const begin = () => {
    debug('START tx');
    return isolationLevel
      ? runner.query(`START TRANSACTION ISOLATION LEVEL ${isolationLevel}`)
      : runner.query('START TRANSACTION');
  };

  const commit = () => {
    debug('COMMIT tx');
    return runner.query('COMMIT');
  };

  const rollback = () => {
    debug('ROLLBACK tx');
    return runner.query('ROLLBACK');
  };

  const run = (method) => (...args) => {
    if (!runner[method]) throw new Error(`No method ${method} found!`);
    debug(`Tx ${method} running`);
    return runner[method](...args);
  };

  return {
    begin,
    commit,
    rollback,
    query: run('query'),
    streamQuery: run('streamQuery'),
    formattedQuery: run('formattedQuery'),
    formattedStreamQuery: run('formattedStreamQuery'),
    explain: run('explain'),
    formattedExplain: run('formattedExplain'),
    insert: run('insert'),
    update: run('update'),
    schema: run('schema'),
    copyFrom: run('copyFrom'),
    copyTo: run('copyTo'),
  };
};
