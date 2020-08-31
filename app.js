import {app, errorHandler, query, uuid, sparqlEscapeUri, sparqlEscapeString, sparqlEscapeDateTime} from 'mu';
import fs from 'fs'

import bodyParser from 'body-parser';

const KNOWN_DOMAINS = [
  'data.lblod.info',
  'data.vlaanderen.be',
  'mu.semte.ch',
  'data.europa.eu',
  'purl.org',
  'www.ontologydesignpatterns.org',
  'www.w3.org',
  'xmlns.com',
  'www.semanticdesktop.org',
  'schema.org',
  'centrale-vindplaats.lblod.info'
]

const PROTOCOLS_TO_RENAME = [
  'http:',
  'https:',
  'ftp:',
  'ftps:'
]

app.use(bodyParser.json({
  type: function (req) {
    return /^application\/json/.test(req.get('content-type'));
  }
}));

app.get('/', function (req, res) {
  res.send('Hello harvesting-url-mirror');
});


app.get('/rename', async (req,res) => {
  const queryResult = await query(`
    SELECT DISTINCT ?subject ?predicate ?object WHERE {
      GRAPH <http://my.new.graph> {
        ?subject ?predicate ?object
      }
    }
  `)
  const triples = queryResult.results.bindings
  const triplesRenamed = renameTriples(triples)
  const fileName = writeToFile(triplesRenamed)
  res.json({file: fileName})
})


function renameTriples(triples) {
  const namesDict = {}
  const renamedTriples = []
  triples.forEach(triple => {
    const {subject, predicate, object} = triple
    const renamedTriple = {}
    if(subject.type == 'uri') {
      if(namesDict[subject.value]) {
        renamedTriple.subject = { value: namesDict[subject.value], type: 'uri'}
      } else if(needsToBeRenamed(subject.value)) {
        const {sameAsTriple, newUri} = renameUri(subject.value, namesDict)
        renamedTriples.push(sameAsTriple)
        renamedTriple.subject = { value: newUri, type: 'uri'}
        namesDict[subject.value] = newUri
      } else {
        renamedTriple.subject = subject
      }
    } else {
      renamedTriple.subject = subject
    }
    renamedTriple.predicate = predicate
    if(subject.type == 'uri') {
      if(namesDict[object.value]) {
        renamedTriple.object = { value: namesDict[object.value], type: 'uri'}
      } else if(needsToBeRenamed(object.value)) {
        const {sameAsTriple, newUri} = renameUri(object.value, namesDict)
        renamedTriples.push(sameAsTriple)
        renamedTriple.object = { value: newUri, type: 'uri'}
        namesDict[object.value] = newUri
      } else {
        renamedTriple.object = object
      }
    } else {
      renamedTriple.subject = subject
    }
    renamedTriples.push(renamedTriple)
  });
  return renamedTriples;
}

function needsToBeRenamed(uri) {
  try {
    const {hostname, protocol} = new URL(uri)
    return hostname && protocol && PROTOCOLS_TO_RENAME.includes(protocol) && !KNOWN_DOMAINS.includes(hostname)
  } catch(e) {
    return false;
  }
}

function renameUri(oldUri) {
  const newUri = `http://centrale-vindplaats.lblod.info/id/${uuid()}`
  const sameAsTriple = {
    subject: {value: newUri, type: 'uri'},
    predicate: {value: 'http://www.w3.org/2002/07/owl#sameAs', type: 'uri'},
    object: {value: oldUri, type: 'uri'}
  }
  return {sameAsTriple, newUri}
}

function writeToFile(triples) {
  const tripleStrings = triples.map((triple) => {
    const subject = processPart(triple.subject)
    const predicate = processPart(triple.predicate)
    const object = processPart(triple.object)
    return `${subject} ${predicate} ${object}.`
  })
  const fileName = `export-${uuid()}.ttl`
  fs.writeFile(`/exports/${fileName}`, tripleStrings.join('\n'), (err) => {
    if(err) throw err
    console.log(err)
    console.log('File saved ' + fileName)
  })
  return fileName
}

function processPart(part) {
  if(part.type === 'uri') {
    if(part.value === '#') return '<http://void>'
    return sparqlEscapeUri(part.value)
  } else if (part.type === 'literal') {
    return sparqlEscapeString(part.value)
  } else if(part.type === 'typed-literal') {
    return `"${part.value}"^^<${part.datatype}>`
  }
}



app.use(errorHandler);