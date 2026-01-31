#!/usr/bin/env npx tsx
/**
 * Debug script for arithmetic expression evaluation
 */

import { Database } from '../../src/database.js';

const db = new Database(':memory:');

// Setup from select1.test
db.exec('CREATE TABLE t1(a INTEGER, b INTEGER, c INTEGER, d INTEGER, e INTEGER)');
db.exec('INSERT INTO t1(e,c,b,d,a) VALUES(103,102,100,101,104)');
db.exec('INSERT INTO t1(a,c,d,e,b) VALUES(107,106,108,109,105)');
db.exec('INSERT INTO t1(e,d,b,a,c) VALUES(110,114,112,111,113)');

console.log('=== Row Data ===');
const rows = db.prepare('SELECT * FROM t1').all();
console.log(rows);

console.log('\n=== Test 1: Basic select ===');
try {
  const result1 = db.prepare('SELECT a, b, c FROM t1').all();
  console.log('Result:', result1);
} catch (e) {
  console.log('Error:', e);
}

console.log('\n=== Test 2: Simple arithmetic a+b ===');
try {
  const result2 = db.prepare('SELECT a+b FROM t1').all();
  console.log('Result:', result2);
} catch (e) {
  console.log('Error:', e);
}

console.log('\n=== Test 3: a+b*2 (operator precedence) ===');
try {
  const result3 = db.prepare('SELECT a+b*2 FROM t1').all();
  console.log('Result:', result3);
} catch (e) {
  console.log('Error:', e);
}

console.log('\n=== Test 4: Complex expression a+b*2+c*3+d*4+e*5 ===');
try {
  const result4 = db.prepare('SELECT a+b*2+c*3+d*4+e*5 FROM t1').all();
  console.log('Result:', result4);
} catch (e) {
  console.log('Error:', e);
}

console.log('\n=== Test 5: Division (a+b+c+d+e)/5 ===');
try {
  const result5 = db.prepare('SELECT (a+b+c+d+e)/5 FROM t1').all();
  console.log('Result:', result5);
} catch (e) {
  console.log('Error:', e);
}

console.log('\n=== Test 6: abs(b-c) function ===');
try {
  const result6 = db.prepare('SELECT abs(b-c) FROM t1').all();
  console.log('Result:', result6);
} catch (e) {
  console.log('Error:', e);
}

console.log('\n=== Test 7: Multiple arithmetic columns ===');
try {
  const result7 = db.prepare('SELECT a+b*2+c*3+d*4+e*5, (a+b+c+d+e)/5 FROM t1').all();
  console.log('Result:', result7);
} catch (e) {
  console.log('Error:', e);
}

console.log('\n=== Test 8: With WHERE clause e>c ===');
console.log('Row data for context:');
console.log('Row 1: a=104, b=100, c=102, d=101, e=103 -> e>c is 103>102 = true');
console.log('Row 2: a=107, b=105, c=106, d=108, e=109 -> e>c is 109>106 = true');
console.log('Row 3: a=111, b=112, c=113, d=114, e=110 -> e>c is 110>113 = false');
try {
  const result8 = db.prepare('SELECT a+b*2 FROM t1 WHERE e>c').all();
  console.log('Result:', result8);
} catch (e) {
  console.log('Error:', e);
}

console.log('\n=== Test 8b: Simpler WHERE e > 105 ===');
try {
  const result8b = db.prepare('SELECT a FROM t1 WHERE e > 105').all();
  console.log('Result:', result8b);
} catch (e) {
  console.log('Error:', e);
}

console.log('\n=== Test 8c: WHERE comparing columns e > c (different form) ===');
try {
  const result8c = db.prepare('SELECT a, e, c FROM t1').all();
  console.log('All rows:', result8c);
  // Check if WHERE clause is interpreting e > c correctly
  const result8d = db.prepare('SELECT a, e, c, e > c AS cmp FROM t1').all();
  console.log('With comparison column:', result8d);
} catch (e) {
  console.log('Error:', e);
}

console.log('\n=== Test 9: Complex WHERE (e>c OR e<d) AND d>e ===');
try {
  const result9 = db.prepare('SELECT a+b*2 FROM t1 WHERE (e>c OR e<d) AND d>e').all();
  console.log('Result:', result9);
} catch (e) {
  console.log('Error:', e);
}

console.log('\n=== Test 10: EXISTS subquery ===');
try {
  const result10 = db.prepare('SELECT a FROM t1 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)').all();
  console.log('Result:', result10);
} catch (e) {
  console.log('Error:', e);
}

console.log('\n=== Test 11: Full failing query from select1.test line 109 ===');
try {
  const result11 = db.prepare(`
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
  console.log('Result:', result11);
} catch (e) {
  console.log('Error:', e);
}
