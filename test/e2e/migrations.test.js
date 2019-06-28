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

  before((done) => {
    pgComponent.start(config, (err, api) => {
      if (err) return done(err);
      formattedQuery = api.formattedQuery;
      return done();
    });
  });

  beforeEach(() => Promise.all([formattedQuery('drop-migrations-test'), formattedQuery('drop-migrations'), formattedQuery('create-migrations-user')]));

  after((done) => {
    pgComponent.stop(done);
  });

  it('initialises migrations as part of a system configuration', (done) => {
    const migratingPg = createPostgres({ configPath: 'withMigrations' });
    migratingPg.start(config, (err, pg) => {
      if (err) return done(err);
      return pg.query('SELECT * FROM migrations')
        .then(() => done())
        .catch(done);
    });
  });

  it('Uses migration credentials if supplied', (done) => {
    const migratingPg = createPostgres({ configPath: 'withMigrationsCredentials' });
    migratingPg.start(config, (err, pg) => {
      if (err) return done(err);
      return pg.query("SELECT tableowner FROM pg_tables WHERE tablename = 'migrations'")
        .then(({ rows }) => {
          expect(rows.length).to.be(1);
          expect(rows[0].tableowner).to.be('migrations');
        })
        .then(() => done())
        .catch(done);
    });
  });

  it('runs all migration steps', (done) => {
    const migratingPg = createPostgres({ configPath: 'withMigrations' });
    migratingPg.start(config, (err, pg) => {
      if (err) return done(err);
      return pg.query('SELECT * FROM handy_test_migrate ORDER BY id')
        .then(({ rows }) => {
          expect(rows.length).to.be(3);
          expect(rows[0].id).to.be(1);
          expect(rows[1].name).to.be('two');
          expect(rows[2].id).to.be(3);
        })
        .then(() => done())
        .catch(done);
    });
  });

  it('uses locks to ensure that only one migration can occur at any given time', (done) => {
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

  it('runs migrations from multiple directories when provided', (done) => {
    const migratingPg = createPostgres({ configPath: 'withMultipleMigrations' });
    migratingPg.start(config, (err, pg) => {
      if (err) return done(err);
      return pg.query('SELECT * FROM handy_test_migrate')
        .then(() => pg.query('SELECT * FROM handy_test_migrate'))
        .then(() => done())
        .catch(done);
    });
  });
});

