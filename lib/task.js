import {
  sparqlEscapeUri,
  sparqlEscapeString,
  sparqlEscapeDateTime,
  uuid,
} from 'mu';
import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';
import { TASK_TYPE, SPARQL_PREFIXES, BASES, ERROR_TYPE } from '../constants';
import { parseResult } from './utils';

export async function isTask(subject) {
  //TODO: move to ask query
  const queryStr = `
   ${SPARQL_PREFIXES}
   SELECT ?subject WHERE {
    GRAPH ?g {
      BIND(${sparqlEscapeUri(subject)} as ?subject)
      ?subject a ${sparqlEscapeUri(TASK_TYPE.value)} .
    }
   }
  `;
  const result = await query(queryStr);
  return result.results.bindings.length;
}

export async function loadTask(subject) {
  const queryTask = `
   ${SPARQL_PREFIXES}
   SELECT DISTINCT ?graph ?task ?id ?job ?created ?modified ?status ?index ?operation ?error WHERE {
    GRAPH ?graph {
      BIND(${sparqlEscapeUri(subject)} as ?task)
      ?task a ${sparqlEscapeUri(TASK_TYPE.value)} .
      ?task
        dct:isPartOf ?job ;
        mu:uuid ?id ;
        dct:created ?created ;
        dct:modified ?modified ;
        adms:status ?status ;
        task:index ?index ;
        task:operation ?operation .
      OPTIONAL { ?task task:error ?error . }
    }
   }
  `;

  const task = parseResult(await query(queryTask))[0];
  if (!task) return null;

  //now fetch the hasMany. Easier to parse these
  const queryParentTasks = `
   ${SPARQL_PREFIXES}
   SELECT DISTINCT ?task ?parentTask WHERE {
     GRAPH ?g {
       BIND(${sparqlEscapeUri(subject)} as ?task)
       ?task cogs:dependsOn ?parentTask .
      }
    }
  `;

  const parentTasks = parseResult(await query(queryParentTasks)).map(
    (row) => row.parentTask
  );
  task.parentSteps = parentTasks;

  const queryResultsContainers = `
   ${SPARQL_PREFIXES}
   SELECT DISTINCT ?task ?resultsContainer WHERE {
     GRAPH ?g {
       BIND(${sparqlEscapeUri(subject)} as ?task)
       ?task task:resultsContainer ?resultsContainer .
      }
    }
  `;

  const resultsContainers = parseResult(
    await query(queryResultsContainers)
  ).map((row) => row.resultsContainer);
  task.resultsContainers = resultsContainers;

  const queryInputContainers = `
   ${SPARQL_PREFIXES}
   SELECT DISTINCT ?task ?inputContainer WHERE {
     GRAPH ?g {
       BIND(${sparqlEscapeUri(subject)} as ?task)
       ?task task:inputContainer ?inputContainer .
      }
    }
  `;

  const inputContainers = parseResult(await query(queryInputContainers)).map(
    (row) => row.inputContainer
  );
  task.inputContainers = inputContainers;
  return task;
}

export async function updateTaskStatus(task, status) {
  await update(`
    ${SPARQL_PREFIXES}
    DELETE {
      GRAPH ?g {
        ?subject adms:status ?status .
        ?subject dct:modified ?modified .
      }
    }
    INSERT {
      GRAPH ?g {
       ?subject adms:status ${sparqlEscapeUri(status)} .
       ?subject dct:modified ${sparqlEscapeDateTime(new Date())} .
      }
    }
    WHERE {
      GRAPH ?g {
        BIND(${sparqlEscapeUri(task.task)} as ?subject)
        ?subject adms:status ?status .
        OPTIONAL { ?subject dct:modified ?modified . }
      }
    }
  `);
}

export async function appendTaskError(task, errorMsg) {
  const id = uuid();
  const uri = BASES.errer`${id}`.value;

  const queryError = `
   ${SPARQL_PREFIXES}
   INSERT DATA {
    GRAPH ${sparqlEscapeUri(task.graph)}{
      ${sparqlEscapeUri(uri)}
        a ${sparqlEscapeUri(ERROR_TYPE.value)} ;
        mu:uuid ${sparqlEscapeString(id)} ;
        oslc:message ${sparqlEscapeString(errorMsg)} .
      ${sparqlEscapeUri(task.task)} task:error ${sparqlEscapeUri(uri)} .
    }
   }
  `;

  await update(queryError);
}
