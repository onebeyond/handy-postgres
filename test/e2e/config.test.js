const expect = require('expect.js');
const createPostgres = require('../..');

const config = {
  pg: {
    user: 'postgres',
    database: 'postgres',
    password: 'password',
    host: process.env.POSTGRES_HOST || 'localhost',
    port: 5432,
    max: 10,
    idleTimeoutMillis: 30000,
  },
  pgAlternate: {
    user: 'postgres',
    database: 'postgres',
    password: 'password',
    host: process.env.POSTGRES_HOST || 'localhost',
    port: 5432,
    max: 10,
    idleTimeoutMillis: 30000,
  },
  pgHost: {
    user: 'postgres',
    database: 'postgres',
    password: 'password',
    host: process.env.POSTGRES_HOST || '127.0.0.1',
    port: 5432,
    max: 10,
    idleTimeoutMillis: 30000,
  },
  nested: {
    pg: {
      user: 'postgres',
      database: 'postgres',
      password: 'password',
      host: process.env.POSTGRES_HOST || 'localhost',
      port: 5432,
      max: 10,
      idleTimeoutMillis: 30000,
    },
  },
};

describe('Handy Postgres config test', () => {
  it('connects to other keys', () => {
    const pgComponent = createPostgres({ configPath: 'pgAlternate' });
    return pgComponent.start(config);
  });

  it('connects to nested keys', () => {
    const pgComponent = createPostgres({ configPath: 'nested.pg' });
    return pgComponent.start(config);
  });

  it('fails if key doesnt exist', () => {
    const pgComponent = createPostgres({ configPath: 'invalid' });
    return pgComponent.start(config)
    .catch((err) => {
      expect(err.message).to.be('Unable to create connection pool, check your configuration.');
    });
  });

  it('connects to a default config', () => {
    const pgComponent = createPostgres({});
    return pgComponent.start(config);
  });

  it('connects to a specific host', () => {
    const pgComponent = createPostgres({ configPath: 'pgHost' });
    return pgComponent.start(config);
  });
});

