import { sparqlEscapeUri, sparqlEscapeString } from 'mu';

/**
 * convert results of select query to an array of objects.
 * courtesy: Niels Vandekeybus & Felix
 * @method parseResult
 * @return {Array}
 */
export function parseResult( result ) {
  if(!(result.results && result.results.bindings.length)) return [];

  const bindingKeys = result.head.vars;
  return result.results.bindings.map((row) => {
    const obj = {};
    bindingKeys.forEach((key) => {
      if(row[key] && row[key].datatype == 'http://www.w3.org/2001/XMLSchema#integer' && row[key].value){
        obj[key] = parseInt(row[key].value);
      }
      else if(row[key] && row[key].datatype == 'http://www.w3.org/2001/XMLSchema#dateTime' && row[key].value){
        obj[key] = new Date(row[key].value);
      }
      else obj[key] = row[key] ? row[key].value:undefined;
    });
    return obj;
  });
};

/*
 * converts array of triples to array of NT-tripels
 * @param {Array}: [{subject: {value, type, datatype }, predicate, object }]
 * @return {Array}: ['<http://subject> <http://predicate> <http://object>.']
 */
export function triplesToNT( triples ){
  return triples.map((triple) => {
    const subject = processPart(triple.subject);
    const predicate = processPart(triple.predicate);
    const object = processPart(triple.object);
    return `${subject} ${predicate} ${object}.`;
  });
}

/**
 * Convert a part of a triple to its string representation
 *
 * @param part the part to be converted
 */
function processPart(part) {
  if(part.type === 'uri') {
    if(part.value === '#') return '<http://void>';
    return sparqlEscapeUri(part.value);
  } else if (part.type === 'literal') {
    return sparqlEscapeString(part.value);
  } else if(part.type === 'typed-literal') {
    return `${sparqlEscapeString(part.value)}^^<${part.datatype}>`;
  }
  return null;
}
