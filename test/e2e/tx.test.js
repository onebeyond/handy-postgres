const expect = require('expect.js');
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

  before((done) => {
    pgComponent.start(config, (err, api) => {
      if (err) return done(err);
      withTransaction = api.withTransaction;
      formattedQuery = api.formattedQuery;
      return done();
    });
  });

  before(() =>
    formattedQuery('drop-films')
      .then(() => formattedQuery('create-films')));

  beforeEach(() => formattedQuery('truncate-films'));

  after((done) => {
    formattedQuery('drop-films')
      .then(() => pgComponent.stop(done))
      .catch(done);
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
});
