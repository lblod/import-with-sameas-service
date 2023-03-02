import * as mu from 'mu';
import * as mas from '@lblod/mu-auth-sudo';
import * as cts from '../constants';
import * as rst from 'rdf-string-ttl';
import * as sjp from 'sparqljson-parse';
import * as N3 from 'n3';
import { NAMESPACES as ns } from '../constants';
import { BASES as bs } from '../constants';
const { literal } = N3.DataFactory;

export async function isTask(subject) {
  const response = await mas.querySudo(`
    ASK {
      ${rst.termToString(subject)} a ${rst.termToString(cts.TASK_TYPE)} .
    }`);
  const parser = new sjp.SparqlJsonParser();
  return parser.parseJsonBoolean(response);
}

export async function loadTask(subject) {
  const taskResponse = await mas.querySudo(`
    ${cts.SPARQL_PREFIXES}
    SELECT DISTINCT ?graph ?task ?id ?job ?created ?modified ?status ?index ?operation ?error WHERE {
      GRAPH ?graph {
        BIND(${rst.termToString(subject)} as ?task)
        ?task
          a ${rst.termToString(cts.TASK_TYPE)} ;
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
    LIMIT 1
  `);
  const parser = new sjp.SparqlJsonParser();
  const task = parser.parseJsonResults(taskResponse)[0];
  if (!task) return;

  const parentTasksResponse = await mas.querySudo(`
   ${cts.SPARQL_PREFIXES}
   SELECT DISTINCT ?task ?parentTask WHERE {
     GRAPH ?g {
       ${rst.termToString(subject)} cogs:dependsOn ?parentTask .
      }
    }
  `);
  const parentTasks = parser
    .parseJsonResults(parentTasksResponse)
    .map((row) => row.parentTask);
  task.parentTasks = parentTasks;

  const resultsContainersResponse = await mas.querySudo(`
   ${cts.SPARQL_PREFIXES}
   SELECT DISTINCT ?task ?resultsContainer WHERE {
     GRAPH ?g {
       ${rst.termToString(subject)} task:resultsContainer ?resultsContainer .
      }
    }
  `);
  const resultsContainers = parser
    .parseJsonResults(resultsContainersResponse)
    .map((row) => row.resultsContainer);
  task.resultsContainers = resultsContainers;

  const inputContainersResponse = await mas.querySudo(`
   ${cts.SPARQL_PREFIXES}
   SELECT DISTINCT ?task ?inputContainer WHERE {
     GRAPH ?g {
       ${rst.termToString(subject)} task:inputContainer ?inputContainer .
      }
    }
  `);
  const inputContainers = parser
    .parseJsonResults(inputContainersResponse)
    .map((row) => row.inputContainer);
  task.inputContainers = inputContainers;

  return task;
}

export async function updateTaskStatus(task, status) {
  const now = literal(new Date().toISOString(), ns.xsd`dateTime`);
  return mas.updateSudo(`
    ${cts.SPARQL_PREFIXES}
    DELETE {
      GRAPH ?g {
        ?subject adms:status ?status .
        ?subject dct:modified ?modified .
      }
    }
    INSERT {
      GRAPH ?g {
       ?subject adms:status ${rst.termToString(status)} .
       ?subject dct:modified ${rst.termToString(now)} .
      }
    }
    WHERE {
      GRAPH ?g {
        BIND(${rst.termToString(task.task)} as ?subject)
        ?subject adms:status ?status .
        OPTIONAL { ?subject dct:modified ?modified . }
      }
    }
  `);
}

export async function appendTaskError(task, errorMsg) {
  const id = literal(mu.uuid());
  const uri = bs.error(id.value);

  return mas.updateSudo(`
   ${cts.SPARQL_PREFIXES}
   INSERT DATA {
    GRAPH ${rst.termToString(task.graph)} {
      ${rst.termToString(uri)}
        a ${rst.termToString(cts.ERROR_TYPE)} ;
        mu:uuid ${rst.termToString(id)} ;
        oslc:message ${rst.termToString(literal(errorMsg))} .
      ${rst.termToString(task.task)} task:error ${rst.termToString(uri)} .
    }
   }
  `);
}
