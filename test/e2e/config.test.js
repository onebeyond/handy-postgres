const expect = require('expect.js');
const createPostgres = require('../..');

const config = {
  pg: {
    user: 'postgres',
    database: 'postgres',
    password: 'password',
    host: 'localhost',
    port: 5432,
    max: 10,
    idleTimeoutMillis: 30000,
  },
  pgAlternate: {
    user: 'postgres',
    database: 'postgres',
    password: 'password',
    host: 'localhost',
    port: 5432,
    max: 10,
    idleTimeoutMillis: 30000,
  },
  pgHost: {
    user: 'postgres',
    database: 'postgres',
    password: 'password',
    host: '127.0.0.1',
    port: 5432,
    max: 10,
    idleTimeoutMillis: 30000,
  },
  nested: {
    pg: {
      user: 'postgres',
      database: 'postgres',
      password: 'password',
      host: 'localhost',
      port: 5432,
      max: 10,
      idleTimeoutMillis: 30000,
    },
  },
};

describe('Handy Postgres config test', () => {
  it('connects to other keys', (done) => {
    const pgComponent = createPostgres({ configPath: 'pgAlternate' });
    pgComponent.start(config, done);
  });

  it('connects to nested keys', (done) => {
    const pgComponent = createPostgres({ configPath: 'nested.pg' });
    pgComponent.start(config, done);
  });

  it('fails if key doesnt exist', (done) => {
    const pgComponent = createPostgres({ configPath: 'invalid' });
    pgComponent.start(config, (err) => {
      expect(err.message).to.be('Unable to create connection pool, check your configuration.');
      done();
    });
  });

  it('connects to a default config', (done) => {
    const pgComponent = createPostgres({});
    pgComponent.start(config, done);
  });

  it('connects to a specific host', (done) => {
    const pgComponent = createPostgres({ configPath: 'pgHost' });
    pgComponent.start(config, done);
  });
});

