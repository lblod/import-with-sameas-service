import * as N3 from 'n3';
import * as cts from '../constants';

/**
 * Store arg could also be a regular array.
 */
export function storeToSparql(store) {
  return storeToString(store, false);
}
export function storeToTtl(store) {
  return storeToString(store, true);
}

function storeToString(store, usePrefixes = false) {
  const options = usePrefixes ? { prefixes: cts.WRITER_PREFIXES } : {};
  const writer = new N3.Writer(options);
  store.forEach((q) => writer.addQuad(q.subject, q.predicate, q.object));
  return new Promise((resolve, reject) => {
    writer.end((err, results) => {
      if (err) reject(err);
      else resolve(results);
    });
  });
}

export function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
