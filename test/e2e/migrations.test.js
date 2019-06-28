const R = require('ramda');
const expect = require('expect.js');
const createPostgres = require('../..');

const withSql = {
  user: 'postgres',
  database: 'postgres',
  password: 'password',
  host: 'localhost',
  port: 5432,
  max: 10,
  idleTimeoutMillis: 30000,
  sql: 'test/fixtures/sql',
};
const config = {
  withSql,
  withMigrations: R.merge(withSql, {
    migrations: { directory: 'test/fixtures/migrations' },
  }),
  withMultipleMigrations: R.merge(withSql, {
    migrations: [
      { directory: 'test/fixtures/migrations' },
      { directory: 'test/fixtures/migrations-additional', namespace: 'separate' },
    ],
  }),
  withMigrationsCredentials: R.merge(withSql, {
    migrationsUser: 'migrations',
    migrationsPassword: 'secret',
    migrations: { directory: 'test/fixtures/migrations' },
  }),
};

describe('Handy pg migrations', () => {
  const pgComponent = createPostgres({ configPath: 'withSql' });
  let formattedQuery;

  before(async () => {
    const api = await pgComponent.start(config);
    formattedQuery = api.formattedQuery;
  });

  beforeEach(() => Promise.all([formattedQuery('drop-migrations-test'), formattedQuery('drop-migrations'), formattedQuery('create-migrations-user')]));

  after(pgComponent.stop);

  it('initialises migrations as part of a system configuration', async () => {
    const migratingPg = createPostgres({ configPath: 'withMigrations' });
    const pg = await migratingPg.start(config);
    await pg.query('SELECT * FROM migrations');
  });

  it('Uses migration credentials if supplied', async () => {
    const migratingPg = createPostgres({ configPath: 'withMigrationsCredentials' });
    const pg = await migratingPg.start(config);
    const { rows } = await pg.query("SELECT tableowner FROM pg_tables WHERE tablename = 'migrations'");
    expect(rows.length).to.be(1);
    expect(rows[0].tableowner).to.be('migrations');
  });

  it('runs all migration steps', async () => {
    const migratingPg = createPostgres({ configPath: 'withMigrations' });
    const pg = await migratingPg.start(config);
    const { rows } = await pg.query('SELECT * FROM handy_test_migrate ORDER BY id')
    expect(rows.length).to.be(3);
    expect(rows[0].id).to.be(1);
    expect(rows[1].name).to.be('two');
    expect(rows[2].id).to.be(3);
  });

  it.skip('uses locks to ensure that only one migration can occur at any given time', (done) => {
    const migratingPg1 = createPostgres({ configPath: 'withMigrations' });
    const migratingPg2 = createPostgres({ configPath: 'withMigrations' });
    let called = 0;
    let error;

    const callDone = (err) => {
      if (err && !error) error = err;
      called++;
      if (called === 2) {
        return done(error);
      }
      return called;
    };

    migratingPg1.start(config, callDone);
    migratingPg2.start(config, callDone);
  });

  it('runs migrations from multiple directories when provided', async () => {
    const migratingPg = createPostgres({ configPath: 'withMultipleMigrations' });
    const { query } = await migratingPg.start(config);
    await query('SELECT * FROM handy_test_migrate');
    await query('SELECT * FROM handy_test_migrate');
  });
});

