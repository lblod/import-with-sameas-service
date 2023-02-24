import * as mu from 'mu';
import * as mas from '@lblod/mu-auth-sudo';
import * as cts from '../constants';
import * as uti from './utils';

export async function isTask(subject) {
  //TODO: move to ask query
  const queryStr = `
   ${cts.SPARQL_PREFIXES}
   SELECT ?subject WHERE {
    GRAPH ?g {
      BIND(${mu.sparqlEscapeUri(subject)} as ?subject)
      ?subject a ${mu.sparqlEscapeUri(cts.TASK_TYPE.value)} .
    }
   }
  `;
  const result = await mas.querySudo(queryStr);
  return result.results.bindings.length;
}

export async function loadTask(subject) {
  const queryTask = `
   ${cts.SPARQL_PREFIXES}
   SELECT DISTINCT ?graph ?task ?id ?job ?created ?modified ?status ?index ?operation ?error WHERE {
    GRAPH ?graph {
      BIND(${mu.sparqlEscapeUri(subject)} as ?task)
      ?task a ${mu.sparqlEscapeUri(cts.TASK_TYPE.value)} .
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

  const task = uti.parseResult(await mas.querySudo(queryTask))[0];
  if (!task) return null;

  //now fetch the hasMany. Easier to parse these
  const queryParentTasks = `
   ${cts.SPARQL_PREFIXES}
   SELECT DISTINCT ?task ?parentTask WHERE {
     GRAPH ?g {
       BIND(${mu.sparqlEscapeUri(subject)} as ?task)
       ?task cogs:dependsOn ?parentTask .
      }
    }
  `;

  const parentTasks = uti
    .parseResult(await mas.querySudo(queryParentTasks))
    .map((row) => row.parentTask);
  task.parentSteps = parentTasks;

  const queryResultsContainers = `
   ${cts.SPARQL_PREFIXES}
   SELECT DISTINCT ?task ?resultsContainer WHERE {
     GRAPH ?g {
       BIND(${mu.sparqlEscapeUri(subject)} as ?task)
       ?task task:resultsContainer ?resultsContainer .
      }
    }
  `;

  const resultsContainers = uti
    .parseResult(await mas.querySudo(queryResultsContainers))
    .map((row) => row.resultsContainer);
  task.resultsContainers = resultsContainers;

  const queryInputContainers = `
   ${cts.SPARQL_PREFIXES}
   SELECT DISTINCT ?task ?inputContainer WHERE {
     GRAPH ?g {
       BIND(${mu.sparqlEscapeUri(subject)} as ?task)
       ?task task:inputContainer ?inputContainer .
      }
    }
  `;

  const inputContainers = uti
    .parseResult(await mas.querySudo(queryInputContainers))
    .map((row) => row.inputContainer);
  task.inputContainers = inputContainers;
  return task;
}

export async function updateTaskStatus(task, status) {
  await mas.updateSudo(`
    ${cts.SPARQL_PREFIXES}
    DELETE {
      GRAPH ?g {
        ?subject adms:status ?status .
        ?subject dct:modified ?modified .
      }
    }
    INSERT {
      GRAPH ?g {
       ?subject adms:status ${mu.sparqlEscapeUri(status)} .
       ?subject dct:modified ${mu.sparqlEscapeDateTime(new Date())} .
      }
    }
    WHERE {
      GRAPH ?g {
        BIND(${mu.sparqlEscapeUri(task.task)} as ?subject)
        ?subject adms:status ?status .
        OPTIONAL { ?subject dct:modified ?modified . }
      }
    }
  `);
}

export async function appendTaskError(task, errorMsg) {
  const id = mu.uuid();
  const uri = cts.BASES.errer`${id}`.value;

  const queryError = `
   ${cts.SPARQL_PREFIXES}
   INSERT DATA {
    GRAPH ${mu.sparqlEscapeUri(task.graph)}{
      ${mu.sparqlEscapeUri(uri)}
        a ${mu.sparqlEscapeUri(cts.ERROR_TYPE.value)} ;
        mu:uuid ${mu.sparqlEscapeString(id)} ;
        oslc:message ${mu.sparqlEscapeString(errorMsg)} .
      ${mu.sparqlEscapeUri(task.task)} task:error ${mu.sparqlEscapeUri(uri)} .
    }
   }
  `;

  await mas.updateSudo(queryError);
}
