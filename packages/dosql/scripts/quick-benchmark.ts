#!/usr/bin/env npx tsx
/**
 * Quick DoSQL Performance Benchmark
 */

import { Database } from '../src/database.js';

// Quick performance test
const db = new Database(':memory:');

// Create test table
db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER, city TEXT)');

// Insert test data
const insert = db.prepare('INSERT INTO users (name, age, city) VALUES (?, ?, ?)');
const cities = ['NYC', 'LA', 'Chicago', 'Houston', 'Phoenix'];
const startInsert = performance.now();
for (let i = 0; i < 1000; i++) {
  insert.run('User' + i, 20 + (i % 50), cities[i % 5]);
}
const insertTime = performance.now() - startInsert;

console.log('=== DoSQL Quick Benchmark ===');
console.log('');
console.log('INSERT 1000 rows:', insertTime.toFixed(2), 'ms', '(' + (1000000/insertTime).toFixed(0) + ' rows/sec)');

// Point query
const startPoint = performance.now();
for (let i = 0; i < 100; i++) {
  db.prepare('SELECT * FROM users WHERE id = ?').all(i * 10);
}
const pointTime = performance.now() - startPoint;
console.log('Point lookup x100:', pointTime.toFixed(2), 'ms', '(' + (100/pointTime*1000).toFixed(0) + ' qps)');

// Range query
const startRange = performance.now();
for (let i = 0; i < 100; i++) {
  db.prepare('SELECT * FROM users WHERE age > ? AND age < ?').all(25, 35);
}
const rangeTime = performance.now() - startRange;
console.log('Range query x100:', rangeTime.toFixed(2), 'ms', '(' + (100/rangeTime*1000).toFixed(0) + ' qps)');

// Aggregate
const startAgg = performance.now();
for (let i = 0; i < 100; i++) {
  db.prepare('SELECT city, COUNT(*), AVG(age) FROM users GROUP BY city').all();
}
const aggTime = performance.now() - startAgg;
console.log('Aggregate x100:', aggTime.toFixed(2), 'ms', '(' + (100/aggTime*1000).toFixed(0) + ' qps)');

// Complex query with subquery
const startComplex = performance.now();
for (let i = 0; i < 50; i++) {
  db.prepare('SELECT * FROM users WHERE age > (SELECT AVG(age) FROM users)').all();
}
const complexTime = performance.now() - startComplex;
console.log('Subquery x50:', complexTime.toFixed(2), 'ms', '(' + (50/complexTime*1000).toFixed(0) + ' qps)');

// CASE expression
const startCase = performance.now();
for (let i = 0; i < 100; i++) {
  db.prepare("SELECT id, CASE WHEN age < 30 THEN 'young' WHEN age < 50 THEN 'middle' ELSE 'senior' END FROM users").all();
}
const caseTime = performance.now() - startCase;
console.log('CASE expr x100:', caseTime.toFixed(2), 'ms', '(' + (100/caseTime*1000).toFixed(0) + ' qps)');

// ORDER BY with LIMIT
const startOrder = performance.now();
for (let i = 0; i < 100; i++) {
  db.prepare('SELECT * FROM users ORDER BY age DESC LIMIT 10').all();
}
const orderTime = performance.now() - startOrder;
console.log('ORDER BY LIMIT x100:', orderTime.toFixed(2), 'ms', '(' + (100/orderTime*1000).toFixed(0) + ' qps)');

// Full table scan
const startScan = performance.now();
for (let i = 0; i < 10; i++) {
  db.prepare('SELECT * FROM users').all();
}
const scanTime = performance.now() - startScan;
console.log('Full scan x10:', scanTime.toFixed(2), 'ms', '(' + (10/scanTime*1000).toFixed(0) + ' qps)');

console.log('');
console.log('Total rows in table:', (db.prepare('SELECT COUNT(*) as c FROM users').all()[0] as any).c);
console.log('');

// Summary
console.log('=== Performance Summary ===');
console.log('Operation         | Latency (avg) | Throughput');
console.log('------------------|---------------|------------');
console.log(`INSERT            | ${(insertTime/1000).toFixed(3)} ms/row   | ${(1000000/insertTime).toFixed(0)} rows/sec`);
console.log(`Point Lookup      | ${(pointTime/100).toFixed(3)} ms/query | ${(100/pointTime*1000).toFixed(0)} qps`);
console.log(`Range Query       | ${(rangeTime/100).toFixed(3)} ms/query | ${(100/rangeTime*1000).toFixed(0)} qps`);
console.log(`Aggregate         | ${(aggTime/100).toFixed(3)} ms/query | ${(100/aggTime*1000).toFixed(0)} qps`);
console.log(`Subquery          | ${(complexTime/50).toFixed(3)} ms/query | ${(50/complexTime*1000).toFixed(0)} qps`);
console.log(`CASE Expression   | ${(caseTime/100).toFixed(3)} ms/query | ${(100/caseTime*1000).toFixed(0)} qps`);
console.log(`ORDER BY LIMIT    | ${(orderTime/100).toFixed(3)} ms/query | ${(100/orderTime*1000).toFixed(0)} qps`);
console.log(`Full Scan (1000)  | ${(scanTime/10).toFixed(3)} ms/query | ${(10/scanTime*1000).toFixed(0)} qps`);
