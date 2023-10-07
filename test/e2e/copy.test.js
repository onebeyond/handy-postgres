const fs = require('fs');
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

describe('Handy pg copy test', () => {
  const pgComponent = createPostgres({ configPath: 'withSql' });
  let pg;

  before(async () => {
    pg = await pgComponent.start(config);
    await pg.formattedQuery('drop-films');
    await pg.formattedQuery('create-films');
  });

  beforeEach(() => pg.formattedQuery('truncate-films'));

  after(async () => {
    await pg.formattedQuery('drop-films');
    await pgComponent.stop();
  });

  it('copies from read stream to table', () => {
    const movie1 = { code: 'kurtz', title: 'Apocalypse Now', dateProd: new Date('1979-08-15'), kind: 'drama', len: 153 };
    const movie2 = { code: 'pulpf', title: 'Pulp Fiction', kind: 'cult', dateProd: null, len: 178 };
    const readStream = fs.createReadStream('test/fixtures/data/films.tsv');
    return pg.copyFrom(readStream, 'films')
    .then(() => pg.formattedQuery('select-all', ['films']))
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
    return pg.copyFrom(readStream, 'films')
    .then(() => pg.copyTo(writeStream, 'films'))
    .then(() => expect(fs.readFileSync(sourcePath)).to.eql(fs.readFileSync(destinationPath)));
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

      await handyPgWrapper.formattedQuery('drop-films');
      await handyPgWrapper.formattedQuery('create-films');
    });

    beforeEach(() => handyPgWrapper.formattedQuery('truncate-films'));

    it('copies from read stream to table', async () => {
      const movie1 = { code: 'kurtz', title: 'Apocalypse Now', dateProd: new Date('1979-08-15'), kind: 'drama', len: 153 };
      const movie2 = { code: 'pulpf', title: 'Pulp Fiction', kind: 'cult', dateProd: null, len: 178 };
      const readStream = fs.createReadStream('test/fixtures/data/films.tsv');
      return handyPgWrapper.copyFrom(readStream, 'films')
      .then(() => handyPgWrapper.formattedQuery('select-all', ['films']))
      .then(({ rows }) => {
        expect(rows.length).to.be(2);
        expect(rows[0]).to.eql(movie1);
        expect(rows[1]).to.eql(movie2);
      });
    });

    it('copies to write stream from table', async () => {
      const sourcePath = 'test/fixtures/data/films.tsv';
      const destinationPath = 'test/fixtures/data/films_out.tsv';
      fs.writeFileSync(destinationPath, ''); // Overwrite file with empty
      const readStream = fs.createReadStream(sourcePath);
      const writeStream = fs.createWriteStream(destinationPath);
      return handyPgWrapper.copyFrom(readStream, 'films')
        .then(() => handyPgWrapper.copyTo(writeStream, 'films'))
        .then(() => expect(fs.readFileSync(sourcePath)).to.eql(fs.readFileSync(destinationPath)));
    });
  });
});
