const R = require('ramda');
const expect = require('expect.js');
const fs = require('fs');
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
    sql: ['test/fixtures/sql/schemas']
  }
};

describe('Handy pg query test for schemas', () => {
  const pgComponent = createPostgres({ configPath: 'withSql' });
  let formattedQuery;
  let insert;
  let update;
  let query;
  let withTransaction;
  let copyFrom;
  let copyTo;

  before(async () => {
    const api = await pgComponent.start(config);
    const schemaApi = api.schema('test_schema');
    formattedQuery = schemaApi.formattedQuery;
    query = schemaApi.query;
    insert = schemaApi.insert;
    update = schemaApi.update;
    withTransaction = schemaApi.withTransaction;
    copyFrom = schemaApi.copyFrom;
    copyTo = schemaApi.copyTo;
    await formattedQuery('create-schema');
    await formattedQuery('drop-films');
    await formattedQuery('create-films');
  });

  beforeEach(() => formattedQuery('truncate-films'));

  after(async () => {
    await formattedQuery('drop-films');
    await pgComponent.stop();
  });

  it('executes a raw query', () =>
    query('SELECT 5 AS value')
      .then(({ rows }) => expect(rows[0].value).to.be(5))
      .catch(err => expect(err).to.be(null)));

  it('executes multipleStatements in a raw query', () =>
    query('SELECT 1 AS one; SELECT 2 AS two;')
      .then(([resultOne, resultTwo]) => {
        expect(resultOne.rows[0].one).to.be(1);
        expect(resultTwo.rows[0].two).to.be(2);
      })
      .catch(err => expect(err).to.be(null)));

  it('complains if an inexistent shorthand is used', () =>
    query('inexistent-shorthand').catch(err =>
      expect(err.message).to.be('syntax error at or near "inexistent"')
    ));

  it('executes parameterised query', () =>
    formattedQuery('select-formatted', [22, 'catch'])
      .then(({ rows }) => expect(R.head(rows).catch).to.equal(22))
      .catch(err => expect(err).to.be(null)));

  it('inserts rows using the shorthand', () => {
    const movie = {
      code: 'kurtz',
      title: 'Apocalypse Now',
      dateProd: new Date('1979-08-15'),
      kind: 'drama',
      len: 153
    };
    return insert('films', movie)
      .then(() => formattedQuery('select-all', ['films']))
      .then(({ rows }) => expect(R.head(rows)).to.eql(movie))
      .catch(err => expect(err).to.be(null));
  });

  it('inserts inside a transaction', () => {
    const movie = {
      code: 'kurtz',
      title: 'Apocalypse Now',
      dateProd: new Date('1979-08-15'),
      kind: 'drama',
      len: 153
    };
    return withTransaction(tx => tx.insert('films', movie))
      .then(() => formattedQuery('select-all', ['films']))
      .then(({ rows }) => expect(R.head(rows)).to.eql(movie))
      .catch(err => expect(err).to.be(null));
  });

  it('updates rows using the shorthand without a where clause', () => {
    const movie1 = {
      code: 'kurtz',
      title: 'Apocalypse Now',
      dateProd: new Date('1979-08-15'),
      kind: 'drama',
      len: 153
    };
    const movie2 = {
      code: 'pulpf',
      title: 'Pulp Fiction',
      kind: 'cult',
      dateProd: null,
      len: 178
    };
    return insert('films', [movie1, movie2])
      .then(() => update('films', { kind: 'super-cool' }))
      .then(() => formattedQuery('select-all', ['films']))
      .then(({ rows }) => {
        expect(rows[0]).to.eql(R.merge(movie1, { kind: 'super-cool' }));
        expect(rows[1]).to.eql(R.merge(movie2, { kind: 'super-cool' }));
      })
      .catch(err => expect(err).to.be(null));
  });

  it('updates rows using the shorthand with a where clause', () => {
    const movie1 = {
      code: 'kurtz',
      title: 'Apocalypse Now',
      dateProd: new Date('1979-08-15'),
      kind: 'drama',
      len: 153
    };
    const movie2 = {
      code: 'pulpf',
      title: 'Pulp Fiction',
      kind: 'cult',
      dateProd: null,
      len: 178
    };
    return insert('films', [movie1, movie2])
      .then(() => update('films', { kind: 'super-cool' }, { code: 'pulpf' }))
      .then(() => formattedQuery('select-all', ['films']))
      .then(({ rows }) => {
        expect(rows[0]).to.eql(movie1);
        expect(rows[1]).to.eql(R.merge(movie2, { kind: 'super-cool' }));
      })
      .catch(err => expect(err).to.be(null));
  });

  it('copies from read stream to table', () => {
    const movie1 = {
      code: 'kurtz',
      title: 'Apocalypse Now',
      dateProd: new Date('1979-08-15'),
      kind: 'drama',
      len: 153
    };
    const movie2 = {
      code: 'pulpf',
      title: 'Pulp Fiction',
      kind: 'cult',
      dateProd: null,
      len: 178
    };
    const readStream = fs.createReadStream('test/fixtures/data/films.tsv');
    return copyFrom(readStream, 'films')
      .then(() => formattedQuery('select-all', ['films']))
      .then(({ rows }) => {
        expect(rows.length).to.be(2);
        expect(rows[0]).to.eql(movie1);
        expect(rows[1]).to.eql(movie2);
      });
  });

  it('copies to write stream from table', () => {
    const sourcePath = 'test/fixtures/data/films.tsv';
    const destinationPath = 'test/fixtures/data/films_out.tsv';
    fs.writeFileSync(destinationPath, ''); // Overwrite file with empty
    const readStream = fs.createReadStream(sourcePath);
    const writeStream = fs.createWriteStream(destinationPath);
    return copyFrom(readStream, 'films')
      .then(() => copyTo(writeStream, 'films'))
      .then(() =>
        expect(fs.readFileSync(sourcePath)).to.eql(
          fs.readFileSync(destinationPath)
        )
      );
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
      handyPgWrapper = handyPgWrapper.schema('test_schema');

      await handyPgWrapper.formattedQuery('create-schema');
      await handyPgWrapper.formattedQuery('drop-films');
      await handyPgWrapper.formattedQuery('create-films');
    });
    
    beforeEach(() => handyPgWrapper.formattedQuery('truncate-films'));

    it('executes a raw query', () =>
      handyPgWrapper.query('SELECT 5 AS value')
        .then(({ rows }) => expect(rows[0].value).to.be(5))
        .catch(err => expect(err).to.be(null)));

    it('executes multipleStatements in a raw query', () =>
      handyPgWrapper.query('SELECT 1 AS one; SELECT 2 AS two;')
        .then(([resultOne, resultTwo]) => {
          expect(resultOne.rows[0].one).to.be(1);
          expect(resultTwo.rows[0].two).to.be(2);
        })
        .catch(err => expect(err).to.be(null)));

    it('complains if an inexistent shorthand is used', () =>
      handyPgWrapper.query('inexistent-shorthand').catch(err =>
        expect(err.message).to.be('syntax error at or near "inexistent"')
      ));

    it('executes parameterised query', () =>
      handyPgWrapper.formattedQuery('select-formatted', [22, 'catch'])
        .then(({ rows }) => expect(R.head(rows).catch).to.equal(22))
        .catch(err => expect(err).to.be(null)));

    it('inserts rows using the shorthand', () => {
      const movie = {
        code: 'kurtz',
        title: 'Apocalypse Now',
        dateProd: new Date('1979-08-15'),
        kind: 'drama',
        len: 153
      };
      return insert('films', movie)
        .then(() => handyPgWrapper.formattedQuery('select-all', ['films']))
        .then(({ rows }) => expect(R.head(rows)).to.eql(movie))
        .catch(err => expect(err).to.be(null));
    });

    it('inserts inside a transaction', () => {
      const movie = {
        code: 'kurtz',
        title: 'Apocalypse Now',
        dateProd: new Date('1979-08-15'),
        kind: 'drama',
        len: 153
      };
      return handyPgWrapper.withTransaction(tx => tx.insert('films', movie))
        .then(() => handyPgWrapper.formattedQuery('select-all', ['films']))
        .then(({ rows }) => expect(R.head(rows)).to.eql(movie))
        .catch(err => expect(err).to.be(null));
    });

    it('updates rows using the shorthand without a where clause', () => {
      const movie1 = {
        code: 'kurtz',
        title: 'Apocalypse Now',
        dateProd: new Date('1979-08-15'),
        kind: 'drama',
        len: 153
      };
      const movie2 = {
        code: 'pulpf',
        title: 'Pulp Fiction',
        kind: 'cult',
        dateProd: null,
        len: 178
      };
      return handyPgWrapper.insert('films', [movie1, movie2])
        .then(() => handyPgWrapper.update('films', { kind: 'super-cool' }))
        .then(() => handyPgWrapper.formattedQuery('select-all', ['films']))
        .then(({ rows }) => {
          expect(rows[0]).to.eql(R.merge(movie1, { kind: 'super-cool' }));
          expect(rows[1]).to.eql(R.merge(movie2, { kind: 'super-cool' }));
        })
        .catch(err => expect(err).to.be(null));
    });

    it('updates rows using the shorthand with a where clause', () => {
      const movie1 = {
        code: 'kurtz',
        title: 'Apocalypse Now',
        dateProd: new Date('1979-08-15'),
        kind: 'drama',
        len: 153
      };
      const movie2 = {
        code: 'pulpf',
        title: 'Pulp Fiction',
        kind: 'cult',
        dateProd: null,
        len: 178
      };
      return handyPgWrapper.insert('films', [movie1, movie2])
        .then(() => handyPgWrapper.update('films', { kind: 'super-cool' }, { code: 'pulpf' }))
        .then(() => handyPgWrapper.formattedQuery('select-all', ['films']))
        .then(({ rows }) => {
          expect(rows[0]).to.eql(movie1);
          expect(rows[1]).to.eql(R.merge(movie2, { kind: 'super-cool' }));
        })
        .catch(err => expect(err).to.be(null));
    });

    it('copies from read stream to table', () => {
      const movie1 = {
        code: 'kurtz',
        title: 'Apocalypse Now',
        dateProd: new Date('1979-08-15'),
        kind: 'drama',
        len: 153
      };
      const movie2 = {
        code: 'pulpf',
        title: 'Pulp Fiction',
        kind: 'cult',
        dateProd: null,
        len: 178
      };
      const readStream = fs.createReadStream('test/fixtures/data/films.tsv');
      return handyPgWrapper.copyFrom(readStream, 'films')
        .then(() => handyPgWrapper.formattedQuery('select-all', ['films']))
        .then(({ rows }) => {
          expect(rows.length).to.be(2);
          expect(rows[0]).to.eql(movie1);
          expect(rows[1]).to.eql(movie2);
        });
    });

    it('copies to write stream from table', () => {
      const sourcePath = 'test/fixtures/data/films.tsv';
      const destinationPath = 'test/fixtures/data/films_out.tsv';
      fs.writeFileSync(destinationPath, ''); // Overwrite file with empty
      const readStream = fs.createReadStream(sourcePath);
      const writeStream = fs.createWriteStream(destinationPath);
      return handyPgWrapper.copyFrom(readStream, 'films')
        .then(() => handyPgWrapper.copyTo(writeStream, 'films'))
        .then(() =>
          expect(fs.readFileSync(sourcePath)).to.eql(
            fs.readFileSync(destinationPath)
          )
        );
    });
  });
});
