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
export async function storeToSparql(store) {
  return await storeToString(store, true);
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
export async function storeToTtl(store) {
  return await storeToString(store, true);
}

/**
 * Converts a store of quads into a string, with or without prefixes.
 *
 * @function
 * @param {N3.Store|Iterable} store - A collection (N3.Store or Array, or
 * something else that can iterated on) that contains RDF.js quads.
 * @param {Boolean} [forSparql = true] - If true, the result will be formatted
 * in N-Triples syntax (without prefixes) so it can be used inside a SPARQL
 * query. If false, the result will contain prefix definitions at the top of
 * the file that will be used throughout the file and will be formatted in
 * more dense Turtle syntax.
 *
 * **NOTE:** realistically, the difference between N-Triples and Turtle syntax
 * is not big. Some datatypes are less explicit in Turtle, such as booleans,
 * which causes mu-authorization to fail.
 */
async function storeToString(store, forSparql = true) {
  // Use the WRITER_PREFIXES, there is much more in them than what is needed
  // for the SPARQL queries.
  const options = forSparql
    ? { format: 'N-Triples' }
    : { format: 'Turtle', prefixes: cts.WRITER_PREFIXES };
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
