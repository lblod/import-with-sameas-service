import * as mu from 'mu';
import * as mas from '@lblod/mu-auth-sudo';
import * as N3 from 'n3';
import {
  STATUS_BUSY,
  STATUS_FAILED,
  STATUS_SUCCESS,
} from '../constants';
import { writeTtlFile } from "./file-helpers";
import { updateTaskStatus, appendTaskError } from './task';
import { getTriples, appendTaskResultGraph } from './graph';
import { triplesToNT } from './utils';
const { namedNode, literal } = N3.DataFactory;

export async function run(task) {
  try {
    await updateTaskStatus(task, STATUS_BUSY);

    const triples = await getTriples(task);
    const complementedTriples = await addMuUUIDs(triples || []);
    console.log(`From before amount to after amount`, triples.length, complementedTriples.length);
    const ntTriples = triplesToNT(complementedTriples);

    const fileContainer = { id: mu.uuid() };
    fileContainer.uri = `http://redpencil.data.gift/id/dataContainers/${task.id}`;
    const mirroredFile = await writeTtlFile(task.graph, ntTriples.join('\n'), 'complemented-triples.ttl');
    await appendTaskResultFile(task, fileContainer, mirroredFile);

    const graphContainer = { id: mu.uuid() };
    graphContainer.uri = `http://redpencil.data.gift/id/dataContainers/${graphContainer.id}`;

    await appendTaskResultGraph(task, graphContainer, fileContainer.uri);
    await updateTaskStatus(task, STATUS_SUCCESS);
  } catch (err) {
    console.error(err);
    await appendTaskError(task, err.message);
    await updateTaskStatus(task, STATUS_FAILED);
  }
}

async function addMuUUIDs(triples) {
  const store = new N3.Store();
  for (const triple of triples) {
    //I know this is silly. Triples are parsed into RDF.js and then transformed
    //into some own format and nom this is doing the opposite again. A store is
    //just much nicer to query.
    //TODO; use RDF.js everywhere?
    let object;
    switch (triple.object.type) {
      case 'uri':
        object = namedNode(triple.object.value);
        break;
      case 'literal':
        object = literal(triple.object.value);
        break;
      case 'typed-literal':
        object = literal(triple.object.value, namedNode(triple.object.datatype.value));
        break;
    }
    if (triple.language)
      object = literal(triple.object.value, triple.object.language);
    store.addQuad(
      namedNode(triple.subject.value),
      namedNode(triple.predicate.value),
      object
    );
  }

  // Get all (unique) subjects from store that have a type.
  const subjects = store.getSubjects(
    namedNode('http://www.w3.org/1999/02/22-rdf-syntax-ns#type')
  );
  for (const sub of subjects) {
    // Does the subject have a UUID already?
    const response = await mas.querySudo(`
      SELECT ?uuid WHERE {
        ${mu.sparqlEscapeUri(sub.value)}
          <http://mu.semte.ch/vocabularies/core/uuid> ?uuid .
      } LIMIT 1
    `);
    const uuid = response?.results?.bindings[0]?.uuid?.value || mu.uuid();
    // Always push a UUID triple on the results.
    triples.push({
      subject: {
        value: sub.value,
        type: 'uri',
      },
      predicate: {
        value: 'http://mu.semte.ch/vocabularies/core/uuid',
        type: 'uri',
      },
      object: {
        value: uuid,
        type: 'literal',
      },
    });
  }
  return triples;
}

async function appendTaskResultFile(task, container, fileUri) {
  const queryStr = `
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
    PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    INSERT DATA {
      GRAPH ${mu.sparqlEscapeUri(task.graph)} {
        ${mu.sparqlEscapeUri(container.uri)} a nfo:DataContainer.
        ${mu.sparqlEscapeUri(container.uri)} mu:uuid ${mu.sparqlEscapeString(container.id)}.
        ${mu.sparqlEscapeUri(container.uri)} task:hasFile ${mu.sparqlEscapeUri(fileUri)}.
        ${mu.sparqlEscapeUri(task.task)} task:resultsContainer ${mu.sparqlEscapeUri(container.uri)}.
      }
    }
  `;

  await mas.updateSudo(queryStr);
}
