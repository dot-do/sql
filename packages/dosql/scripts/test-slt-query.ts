import { Database } from '../src/database.js';

const db = new Database(':memory:');

// Create table with test data
db.exec('CREATE TABLE t1(a INTEGER, b INTEGER, c INTEGER, d INTEGER, e INTEGER)');
db.exec('INSERT INTO t1(e,c,b,d,a) VALUES(103,102,100,101,104)');
db.exec('INSERT INTO t1(a,c,d,e,b) VALUES(107,106,108,109,105)');
db.exec('INSERT INTO t1(e,d,b,a,c) VALUES(110,114,112,111,113)');
db.exec('INSERT INTO t1(d,c,e,a,b) VALUES(116,119,117,115,118)');
db.exec('INSERT INTO t1(c,d,b,e,a) VALUES(123,122,124,120,121)');
db.exec('INSERT INTO t1(a,d,b,e,c) VALUES(127,128,129,126,125)');
db.exec('INSERT INTO t1(e,c,a,d,b) VALUES(132,134,131,133,130)');
db.exec('INSERT INTO t1(a,d,b,e,c) VALUES(138,136,139,135,137)');
db.exec('INSERT INTO t1(e,c,d,a,b) VALUES(144,141,140,142,143)');
db.exec('INSERT INTO t1(b,a,e,d,c) VALUES(145,149,146,148,147)');

console.log('=== Row 1 data ===');
const row = db.prepare('SELECT * FROM t1 LIMIT 1').all()[0];
console.log(row);

// Test the failing CASE expression
console.log('\n=== Test CASE WHEN a<b-3 ===');
try {
  const caseResult = db.prepare(`
    SELECT
      a, b,
      CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222 WHEN a<b+3 THEN 333 ELSE 444 END as result,
      b-3 as "b_minus_3"
    FROM t1
  `).all();
  console.log(caseResult);
} catch (e) {
  console.error('Error:', (e as Error).message);
}

// Test the WHERE clause parts
console.log('\n=== Test WHERE (e>c OR e<d) AND d>e ===');
try {
  const whereResult = db.prepare(`
    SELECT a, b, c, d, e
    FROM t1
    WHERE (e>c OR e<d) AND d>e
  `).all();
  console.log('Rows matching:', whereResult.length);
  console.log(whereResult);
} catch (e) {
  console.error('Error:', (e as Error).message);
}

// Check manual calculation
console.log('\n=== Manual WHERE check ===');
const all = db.prepare('SELECT * FROM t1').all() as Array<{a: number, b: number, c: number, d: number, e: number}>;
for (const r of all) {
  const eGtC = r.e > r.c;
  const eLtD = r.e < r.d;
  const dGtE = r.d > r.e;
  const matches = (eGtC || eLtD) && dGtE;
  console.log(`a=${r.a} e=${r.e} c=${r.c} d=${r.d} | e>c=${eGtC} e<d=${eLtD} d>e=${dGtE} => ${matches}`);
}

// Full failing query
console.log('\n=== Full failing query (simplified) ===');
try {
  const result = db.prepare(`
    SELECT a+b*2+c*3+d*4+e*5,
           CASE WHEN a<b-3 THEN 111 WHEN a<=b THEN 222
            WHEN a<b+3 THEN 333 ELSE 444 END,
           abs(b-c),
           (a+b+c+d+e)/5,
           a+b*2+c*3
      FROM t1
     WHERE (e>c OR e<d)
       AND d>e
       AND EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
     ORDER BY 4,2,1,3,5
  `).all();
  console.log('Result count:', result.length);
  console.log(result);
} catch (e) {
  console.error('Error:', (e as Error).message);
}
