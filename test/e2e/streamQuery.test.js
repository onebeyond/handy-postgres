const R = require('ramda');
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
    sql: 'test/fixtures/sql',
  },
};

const collect = (stream) =>
  new Promise((resolve, reject) => {
    const results = [];
    stream.on('data', (data) => {
      results.push(data);
    });
    stream.on('error', reject);
    stream.on('end', () => resolve({ rows: results }));
  });

describe('Handy pg stream query', () => {
  const pgComponent = createPostgres({ configPath: 'withSql' });
  let streamQuery;
  let formattedQuery;
  let formattedStreamQuery;
  let withTransaction;
  let query;

  before(async () => {
    const api = await pgComponent.start(config);
    streamQuery = api.streamQuery;
    query = api.query;
    formattedQuery = api.formattedQuery;
    formattedStreamQuery = api.formattedStreamQuery;
    withTransaction = api.withTransaction;
  });

  beforeEach(() =>
    formattedQuery('drop-films')
      .then(() => formattedQuery('create-films')));

  after(() => pgComponent.stop());

  it('executes a raw query', () =>
     streamQuery('SELECT 5 AS value')
       .then((readStream) => collect(readStream))
       .then(({ rows }) => expect(rows[0].value).to.be(5)));

  it('executes a raw INSERT query', () =>
     streamQuery("INSERT INTO films (code, title, len) VALUES('irman', 'Iron Man', 138)")
       .then((readStream) => collect(readStream))
       .then(({ rows }) => expect(rows).to.eql([]))
       .then(() => query('SELECT code FROM films'))
       .then(({ rows }) => expect(rows).to.eql([{ code: 'irman' }])));

  const insertReturning = 'INSERT INTO films (code, title, len) ' +
        "VALUES('irman', 'Iron Man', 138) RETURNING code";
  it('executes a raw INSERT RETURNING query', () =>
     streamQuery(insertReturning)
       .then((readStream) => collect(readStream))
       .then(({ rows }) => expect(rows).to.eql([{ code: 'irman' }]))
       .then(() => query('SELECT title FROM films'))
       .then(({ rows }) => expect(rows).to.eql([{ title: 'Iron Man' }])));

  it('executes a raw UPDATE query', () =>
     query("INSERT INTO films (code, title, len) VALUES('irman', 'Iron Man', 138)")
       .then(() => streamQuery("UPDATE films SET len = 210 WHERE code = 'irman'"))
       .then((readStream) => collect(readStream))
       .then(({ rows }) => expect(rows).to.eql([]))
       .then(() => query('SELECT len FROM films'))
       .then(({ rows }) => expect(rows).to.eql([{ len: 210 }])));

  it('executes a raw DELETE query', () =>
     query("INSERT INTO films (code, title, len) VALUES('irman', 'Iron Man', 138)")
       .then(() => query("INSERT INTO films (code, title, len) VALUES('cento', 'Cento', 100)"))
       .then(() => streamQuery("DELETE FROM films WHERE code = 'irman'"))
       .then((readStream) => collect(readStream))
       .then(({ rows }) => expect(rows).to.eql([]))
       .then(() => query('SELECT code FROM films'))
       .then(({ rows }) => expect(rows).to.eql([{ code: 'cento' }])));

  it('works with multiple rows returned', () =>
     query("INSERT INTO films (code, title, len) VALUES('irman', 'Iron Man', 138)")
       .then(() => query("INSERT INTO films (code, title, len) VALUES('cento', 'Cento', 100)"))
       .then(() => streamQuery('SELECT code FROM films'))
       .then((readStream) => collect(readStream))
       .then(({ rows }) => expect(rows).to.eql([{ code: 'irman' }, { code: 'cento' }])));

  it('works with zero rows returned', () =>
     streamQuery('SELECT code FROM films')
       .then((readStream) => collect(readStream))
       .then(({ rows }) => expect(rows).to.eql([])));

  it('complains if multiple statements are in a raw query', () =>
     streamQuery('SELECT 1 AS one; SELECT 2 AS two;')
       .catch((err) => expect(err.message).to.contain('cannot insert multiple commands')));

  it('executes parameterised unformatted query', () =>
     streamQuery('select-parameterised', 100)
       .then((readStream) => collect(readStream))
       .then(({ rows }) => expect(R.head(rows).number).to.equal(100)));

  it('complains if an inexistent shorthand is used', () =>
     streamQuery('inexistent-shorthand')
       .catch((err) => expect(err.message).to.be('syntax error at or near "inexistent"')));

  it('executes a shorthand unformatted query', () =>
     streamQuery('select-1')
       .then((readStream) => collect(readStream))
       .then(({ rows }) => expect(R.head(rows).number).to.equal(1)));

  it('executes parameterised query', () =>
     formattedStreamQuery('select-formatted', [22, 'catch'])
       .then((readStream) => collect(readStream))
       .then(({ rows }) => expect(R.head(rows).catch).to.equal(22)));

  it('complains if multiple statements in parameterised query', () =>
     formattedStreamQuery('select-formatted-multi', [22, 'catch', 23, 'me'])
       .catch((err) => expect(err.message).to.contain('cannot insert multiple commands')));

  it('executes a formatted stream query inside a transaction', () => {
    const pf = { code: 'pulpf', title: 'Pulp Fiction', kind: 'cult', dateProd: null, len: 178 };
    return withTransaction((tx) => tx.insert('films', pf)
                           .then(() => tx.formattedStreamQuery('select-all', ['films']))
                           // Notably has to be inside the transaction block, this will hang
                           // if attempting to read from it after transaction committed
                           .then((readStream) => collect(readStream)))
      .then(({ rows }) => expect(R.head(rows).title).to.equal('Pulp Fiction'));
  });

  it('executes a stream query inside a transaction', () => {
    const pf = { code: 'pulpf', title: 'Pulp Fiction', kind: 'cult', dateProd: null, len: 178 };
    return withTransaction((tx) => tx.insert('films', pf)
                           .then(() => tx.streamQuery('SELECT * FROM films'))
                           // Notably has to be inside the transaction block, this will hang
                           // if attempting to read from it after transaction committed
                           .then((readStream) => collect(readStream)))
      .then(({ rows }) => expect(R.head(rows).title).to.equal('Pulp Fiction'));
  });

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
    });

    beforeEach(async () => {
      await handyPgWrapper.formattedQuery('drop-films');
      await handyPgWrapper.formattedQuery('create-films');
    });

    it('executes a raw query', () =>
      handyPgWrapper.streamQuery('SELECT 5 AS value')
        .then((readStream) => collect(readStream))
        .then(({ rows }) => expect(rows[0].value).to.be(5)));

    it('executes a raw INSERT query', () =>
      handyPgWrapper.streamQuery("INSERT INTO films (code, title, len) VALUES('irman', 'Iron Man', 138)")
        .then((readStream) => collect(readStream))
        .then(({ rows }) => expect(rows).to.eql([]))
        .then(() => query('SELECT code FROM films'))
        .then(({ rows }) => expect(rows).to.eql([{ code: 'irman' }])));

    const insertReturning = 'INSERT INTO films (code, title, len) ' +
      "VALUES('irman', 'Iron Man', 138) RETURNING code";
    it('executes a raw INSERT RETURNING query', () =>
      handyPgWrapper.streamQuery(insertReturning)
        .then((readStream) => collect(readStream))
        .then(({ rows }) => expect(rows).to.eql([{ code: 'irman' }]))
        .then(() => query('SELECT title FROM films'))
        .then(({ rows }) => expect(rows).to.eql([{ title: 'Iron Man' }])));

    it('executes a raw UPDATE query', () =>
      query("INSERT INTO films (code, title, len) VALUES('irman', 'Iron Man', 138)")
        .then(() => handyPgWrapper.streamQuery("UPDATE films SET len = 210 WHERE code = 'irman'"))
        .then((readStream) => collect(readStream))
        .then(({ rows }) => expect(rows).to.eql([]))
        .then(() => query('SELECT len FROM films'))
        .then(({ rows }) => expect(rows).to.eql([{ len: 210 }])));

    it('executes a raw DELETE query', () =>
      query("INSERT INTO films (code, title, len) VALUES('irman', 'Iron Man', 138)")
        .then(() => query("INSERT INTO films (code, title, len) VALUES('cento', 'Cento', 100)"))
        .then(() => handyPgWrapper.streamQuery("DELETE FROM films WHERE code = 'irman'"))
        .then((readStream) => collect(readStream))
        .then(({ rows }) => expect(rows).to.eql([]))
        .then(() => query('SELECT code FROM films'))
        .then(({ rows }) => expect(rows).to.eql([{ code: 'cento' }])));

    it('works with multiple rows returned', () =>
      query("INSERT INTO films (code, title, len) VALUES('irman', 'Iron Man', 138)")
        .then(() => query("INSERT INTO films (code, title, len) VALUES('cento', 'Cento', 100)"))
        .then(() => handyPgWrapper.streamQuery('SELECT code FROM films'))
        .then((readStream) => collect(readStream))
        .then(({ rows }) => expect(rows).to.eql([{ code: 'irman' }, { code: 'cento' }])));

    it('works with zero rows returned', () =>
      handyPgWrapper.streamQuery('SELECT code FROM films')
        .then((readStream) => collect(readStream))
        .then(({ rows }) => expect(rows).to.eql([])));

    it('complains if multiple statements are in a raw query', () =>
      handyPgWrapper.streamQuery('SELECT 1 AS one; SELECT 2 AS two;')
        .catch((err) => expect(err.message).to.contain('cannot insert multiple commands')));

    it('executes parameterised unformatted query', () =>
      handyPgWrapper.streamQuery('select-parameterised', 100)
        .then((readStream) => collect(readStream))
        .then(({ rows }) => expect(R.head(rows).number).to.equal(100)));

    it('complains if an inexistent shorthand is used', () =>
      handyPgWrapper.streamQuery('inexistent-shorthand')
        .catch((err) => expect(err.message).to.be('syntax error at or near "inexistent"')));

    it('executes a shorthand unformatted query', () =>
      handyPgWrapper.streamQuery('select-1')
        .then((readStream) => collect(readStream))
        .then(({ rows }) => expect(R.head(rows).number).to.equal(1)));

    it('executes parameterised query', () =>
      handyPgWrapper.formattedStreamQuery('select-formatted', [22, 'catch'])
        .then((readStream) => collect(readStream))
        .then(({ rows }) => expect(R.head(rows).catch).to.equal(22)));

    it('complains if multiple statements in parameterised query', () =>
      handyPgWrapper.formattedStreamQuery('select-formatted-multi', [22, 'catch', 23, 'me'])
        .catch((err) => expect(err.message).to.contain('cannot insert multiple commands')));

    it('executes a formatted stream query inside a transaction', () => {
      const pf = { code: 'pulpf', title: 'Pulp Fiction', kind: 'cult', dateProd: null, len: 178 };
      return handyPgWrapper.withTransaction((tx) => tx.insert('films', pf)
        .then(() => tx.formattedStreamQuery('select-all', ['films']))
        // Notably has to be inside the transaction block, this will hang
        // if attempting to read from it after transaction committed
        .then((readStream) => collect(readStream)))
        .then(({ rows }) => expect(R.head(rows).title).to.equal('Pulp Fiction'));
    });

    it('executes a stream query inside a transaction', () => {
      const pf = { code: 'pulpf', title: 'Pulp Fiction', kind: 'cult', dateProd: null, len: 178 };
      return handyPgWrapper.withTransaction((tx) => tx.insert('films', pf)
        .then(() => tx.streamQuery('SELECT * FROM films'))
        // Notably has to be inside the transaction block, this will hang
        // if attempting to read from it after transaction committed
        .then((readStream) => collect(readStream)))
        .then(({ rows }) => expect(R.head(rows).title).to.equal('Pulp Fiction'));
    });

  });
});
