const expect = require('expect.js');
const originalPg = require('pg');
const createPostgres = require('../..');

const config = {
  withSql: {
    user: 'postgres',
    database: 'postgres',
    password: 'password',
    host: 'localhost',
    port: 5432,
    max: 10,
    idleTimeoutMillis: 30000,
    isolationLevel: 'REPEATABLE READ',
    sql: 'test/fixtures/sql',
  },
};

describe('Handy pg transactions test', () => {
  const pgComponent = createPostgres({ configPath: 'withSql' });
  let withTransaction;
  let formattedQuery;

  const pf = { code: 'pulpf', title: 'Pulp Fiction', kind: 'cult', dateProd: null, len: 178 };
  const rt = { code: 'royal', title: 'The Royal Tenenbaums', dateProd: new Date('2002-01-04'), kind: 'comedy', len: 110 };
  const lb = { code: 'corey', title: 'The Lost Boys', dateProd: new Date('1987-07-31'), kind: 'drama', len: 97 };

  before(async () => {
    const api = await pgComponent.start(config);
    withTransaction = api.withTransaction;
    formattedQuery = api.formattedQuery;
    await formattedQuery('drop-films');
    await formattedQuery('create-films');
  });

  beforeEach(() => formattedQuery('truncate-films'));

  after(async () => {
    await formattedQuery('drop-films');
    await pgComponent.stop();
  });

  it('inserts a record inside a transaction', () =>
    withTransaction((tx) => tx.insert('films', pf))
    .then(({ command }) => expect(command).to.equal('INSERT'))
    .then(() => formattedQuery('select-all', ['films']))
    .then(({ rows }) => expect(rows.length).to.equal(1))
    .catch((err) => expect(err).to.be(null))
  );

  it('uses the transaction API namespaced by a schema', () =>
    withTransaction((tx) => tx.schema('public').insert('films', pf))
    .then(({ command }) => expect(command).to.equal('INSERT'))
    .then(() => formattedQuery('select-all', ['films']))
    .then(({ rows }) => expect(rows.length).to.equal(1))
    .catch((err) => expect(err).to.be(null))
  );

  it('commits multiple parallel operations within a transaction', () =>
    withTransaction((tx) =>
      Promise.all([
        tx.insert('films', pf),
        tx.insert('films', rt),
      ])
    )
    .then(() =>
      formattedQuery('select-all', ['films'])
      .then(({ rows }) => {
        const [first, second] = rows;
        expect(first.code).to.equal('pulpf');
        expect(second.code).to.equal('royal');
      })
    )
  );

  it('commits multiple serial operations within a transaction', () =>
    withTransaction((tx) =>
      tx.insert('films', pf)
      .then(() => tx.insert('films', rt))
      .then(() => tx.insert('films', lb))
      .then(() => tx.query('DELETE FROM films WHERE code=\'pulpf\''))
    )
    .then(() =>
      formattedQuery('select-all', ['films'])
      .then(({ rows }) => {
        const [first, second] = rows;
        expect(rows.length).to.equal(2);
        expect(first.code).to.equal('royal');
        expect(second.code).to.equal('corey');
      })
    )
  );

  it('automatically rolls back if an error occurs within a transaction', () =>
    withTransaction((tx) =>
      tx.insert('films', pf)
      .then(() => tx.insert('films', pf))
    )
    .catch((err) => expect(err.message).to.be('duplicate key value violates unique constraint "firstkey"'))
    .then(() =>
      formattedQuery('select-all', ['films'])
      .then(({ rows }) => {
        expect(rows.length).to.equal(0);
      })
    )
  );

  describe('Using an external pool', () => {
    const sqlPath = config.withSql.sql;
    const configSql = {
      sql: {
        sql: sqlPath
      },
    };

    let handyPostgres;
    let pool;
    let handyPgWrapper;

    before(async () => {
      handyPostgres = createPostgres({ configPath: 'sql' });
      pool = new originalPg.Pool(config.withSql);
      handyPgWrapper = await handyPostgres.start(configSql, pool);

      await handyPgWrapper.formattedQuery('drop-films');
      await handyPgWrapper.formattedQuery('create-films');
    });
    
    beforeEach(async () => {
      await handyPgWrapper.formattedQuery('truncate-films');
    });

    it('inserts a record inside a transaction', () =>
      handyPgWrapper.withTransaction((tx) => tx.insert('films', pf))
        .then(({ command }) => expect(command).to.equal('INSERT'))
        .then(() => formattedQuery('select-all', ['films']))
        .then(({ rows }) => expect(rows.length).to.equal(1))
        .catch((err) => expect(err).to.be(null))
    );

    it('uses the transaction API namespaced by a schema', () =>
      handyPgWrapper.withTransaction((tx) => tx.schema('public').insert('films', pf))
        .then(({ command }) => expect(command).to.equal('INSERT'))
        .then(() => formattedQuery('select-all', ['films']))
        .then(({ rows }) => expect(rows.length).to.equal(1))
        .catch((err) => expect(err).to.be(null))
    );

    it('commits multiple parallel operations within a transaction', () =>
      handyPgWrapper.withTransaction((tx) =>
        Promise.all([
          tx.insert('films', pf),
          tx.insert('films', rt),
        ])
      )
        .then(() =>
          formattedQuery('select-all', ['films'])
            .then(({ rows }) => {
              const [first, second] = rows;
              expect(first.code).to.equal('pulpf');
              expect(second.code).to.equal('royal');
            })
        )
    );

    it('commits multiple serial operations within a transaction', () =>
      handyPgWrapper.withTransaction((tx) =>
        tx.insert('films', pf)
          .then(() => tx.insert('films', rt))
          .then(() => tx.insert('films', lb))
          .then(() => tx.query('DELETE FROM films WHERE code=\'pulpf\''))
      )
        .then(() =>
          formattedQuery('select-all', ['films'])
            .then(({ rows }) => {
              const [first, second] = rows;
              expect(rows.length).to.equal(2);
              expect(first.code).to.equal('royal');
              expect(second.code).to.equal('corey');
            })
        )
    );

    it('automatically rolls back if an error occurs within a transaction', () =>
      handyPgWrapper.withTransaction((tx) =>
        tx.insert('films', pf)
          .then(() => tx.insert('films', pf))
      )
        .catch((err) => expect(err.message).to.be('duplicate key value violates unique constraint "firstkey"'))
        .then(() =>
          formattedQuery('select-all', ['films'])
            .then(({ rows }) => {
              expect(rows.length).to.equal(0);
            })
        )
    );

  });
});
