import * as N3 from 'n3';

/**
 * Store arg could also be a regular array.
 */
export function storeToTtl(store) {
  const writer = new N3.Writer();
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
