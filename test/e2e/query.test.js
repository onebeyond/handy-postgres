const R = require('ramda');
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
    sql: 'test/fixtures/sql',
  },
};

describe('Handy pg query test', () => {
  const pgComponent = createPostgres({ configPath: 'withSql' });
  let formattedQuery;
  let insert;
  let update;
  let query;
  let explain;
  let formattedExplain;

  before((done) => {
    pgComponent.start(config, (err, api) => {
      if (err) return done(err);
      formattedQuery = api.formattedQuery;
      query = api.query;
      insert = api.insert;
      update = api.update;
      explain = api.explain;
      formattedExplain = api.formattedExplain;
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

  it('executes a raw query', () =>
    query('SELECT 5 AS value')
    .then(({ rows }) => expect(rows[0].value).to.be(5))
    .catch((err) => expect(err).to.be(null))
  );

  it('executes multipleStatements in a raw query', () =>
    query('SELECT 1 AS one; SELECT 2 AS two;')
    .then(({ rows }) => {
      expect(rows[0].one).to.be(1);
      expect(rows[1].two).to.be(2);
    })
    .catch((err) => expect(err).to.be(null))
  );

  it('executes parameterised unformatted query', () =>
    query('select-parameterised', 100)
    .then(({ rows }) => expect(R.head(rows).number).to.equal(100))
    .catch((err) => expect(err).to.be(null))
  );

  it('complains if an inexistent shorthand is used', () =>
    query('inexistent-shorthand')
    .catch((err) => expect(err.message).to.be('syntax error at or near "inexistent"'))
  );

  it('executes a shorthand unformatted query', () =>
    query('select-1')
    .then(({ rows }) => expect(R.head(rows).number).to.equal(1))
    .catch((err) => expect(err).to.be(null))
  );

  it('executes parameterised query', () =>
    formattedQuery('select-formatted', [22, 'catch'])
    .then(({ rows }) => expect(R.head(rows).catch).to.equal(22))
    .catch((err) => expect(err).to.be(null))
  );

  it('inserts rows using the shorthand', () => {
    const movie = { code: 'kurtz', title: 'Apocalypse Now', dateProd: new Date('1979-08-15'), kind: 'drama', len: 153 };
    return insert('films', movie)
    .then(() => formattedQuery('select-all', ['films']))
    .then(({ rows }) => expect(R.head(rows)).to.eql(movie))
    .catch((err) => expect(err).to.be(null));
  });

  it('updates rows using the shorthand without a where clause', () => {
    const movie1 = { code: 'kurtz', title: 'Apocalypse Now', dateProd: new Date('1979-08-15'), kind: 'drama', len: 153 };
    const movie2 = { code: 'pulpf', title: 'Pulp Fiction', kind: 'cult', dateProd: null, len: 178 };
    return insert('films', [movie1, movie2])
    .then(() => update('films', { kind: 'super-cool' }))
    .then(() => formattedQuery('select-all', ['films']))
    .then(({ rows }) => {
      expect(rows[0]).to.eql(R.merge(movie1, { kind: 'super-cool' }));
      expect(rows[1]).to.eql(R.merge(movie2, { kind: 'super-cool' }));
    })
    .catch((err) => expect(err).to.be(null));
  });

  it('updates rows using the shorthand with a where clause', () => {
    const movie1 = { code: 'kurtz', title: 'Apocalypse Now', dateProd: new Date('1979-08-15'), kind: 'drama', len: 153 };
    const movie2 = { code: 'pulpf', title: 'Pulp Fiction', kind: 'cult', dateProd: null, len: 178 };
    return insert('films', [movie1, movie2])
    .then(() => update('films', { kind: 'super-cool' }, { code: 'pulpf' }))
    .then(() => formattedQuery('select-all', ['films']))
    .then(({ rows }) => {
      expect(rows[0]).to.eql(movie1);
      expect(rows[1]).to.eql(R.merge(movie2, { kind: 'super-cool' }));
    })
    .catch((err) => expect(err).to.be(null));
  });

  it('plans a parameterised unformatted query', () =>
    explain('select-parameterised', 100)
    .then(({ rows }) => expect(R.head(rows)).to.have.key('QUERY PLAN'))
    .catch((err) => expect(err).to.be(null))
  );

  it('plans a parameterised formatted query', () =>
    formattedExplain('select-formatted', [22, 'catch'])
    .then(({ rows }) => expect(R.head(rows)).to.have.key('QUERY PLAN'))
    .catch((err) => expect(err).to.be(null))
  );
});
