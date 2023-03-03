import * as N3 from 'n3';
import * as cts from '../constants';

/**
 * Convert a store of quads into a string that can be inserted in a SPARQL
 * query.
 *
 * @public
 * @function
 * @param {N3.Store|Iterable} store - A collection (N3.Store or Array, or
 * something else that can iterated on) that contains RDF.js quads.
 * @returns {String} The triples in SPARQL body form. (No graphs and no
 * prefixes.)
 */
export function storeToSparql(store) {
  return storeToString(store, false);
}

/**
 * Convert a store of quads into a string that that is better suited to writing
 * to a file.
 *
 * @public
 * @function
 * @param {N3.Store|Iterable} store - A collection (N3.Store or Array, or
 * something else that can iterated on) that contains RDF.js quads.
 * @returns {String} The triples in Turtle form. (No graphs, but it uses
 * prefixes at the top to make the file more human readable.)
 */
export function storeToTtl(store) {
  return storeToString(store, true);
}

/**
 * Converts a store of quads into a string, with or without prefixes.
 *
 * @function
 * @param {N3.Store|Iterable} store - A collection (N3.Store or Array, or
 * something else that can iterated on) that contains RDF.js quads.
 * @param {Boolean} [usePrefixes = false] - If true, the result will contain
 * prefix definitions at the top of the file that will be used throughout the
 * file.
 */
function storeToString(store, usePrefixes = false) {
  // Use the WRITER_PREFIXES, there is much more in them than what is needed
  // for the SPARQL queries.
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

/**
 * Simple. Sleep for the given amount of milliseconds.
 *
 * @public
 * @async
 * @function
 * @param {Integer} ms - The amount of milliseconds to sleep for.
 * @returns {undefined} Nothing. (Note that this is an async function, so you
 * should `await` it or use the returned promise to wait for its resolution.)
 */
export function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
