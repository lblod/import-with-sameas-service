import * as cts from './constants';
import * as tsk from './lib/task';
import * as N3 from 'n3';
import bodyParser from 'body-parser';
import { NAMESPACES as ns } from './constants';
import { app, errorHandler } from 'mu';
import { run as runMirrorPipeline } from './lib/pipeline-mirroring';
import { run as runPublishPipeline } from './lib/pipeline-publishing';
import { run as runAddUUIDs } from './lib/pipeline-add-uuids';
import { run as runExecuteDiffDeletesPipeline } from './lib/pipeline-execute-diff-deletes';
const { namedNode } = N3.DataFactory;

app.use(
  bodyParser.json({
    type: function (req) {
      return /^application\/json/.test(req.get('content-type'));
    },
  })
);

/**
 * This function is meant to run on startup of the service to find unfinished
 * tasks (busy or scheduled) and start them again from scratch. This is not
 * like (function ...)(); because this function needs to be addressed via an
 * API call.
 */
async function findAndStartUnfinishedTasks() {
  const unfinishedTasks = await tsk.getUnfinishedTasks();
  for (const term of unfinishedTasks) await processTask(term);
}

/**
 * Run on startup.
 */
findAndStartUnfinishedTasks();

app.get('/', function (_, res) {
  res.send('Hello harvesting-import-sameas-service');
});

app.post('/find-and-start-unfinished-tasks', async function (req, res) {
  res
    .json({ status: 'Finding and restarting unfinished tasks' })
    .status(200)
    .end();
  await findAndStartUnfinishedTasks();
});

app.post('/force-retry-task', async function (req, res) {
  const taskUri = req.body?.uri;
  if (!taskUri)
    res.status(400).send({
      status:
        'No task URI given in the request body. Please send a JSON body with a `status` key and a task URI as value.',
    });
  res.status(200).send({ status: `Force restarting task \`${taskUri}\`` });
  await processTask(namedNode(taskUri));
});

app.post('/delta', async function (req, res) {
  // The delta notifier does not care about the result. Just return as soon as
  // possible.
  res.status(200).send().end();
  try {
    // Filter for triples in the body that are inserts about a task with a
    // status 'scheduled'.
    const taskSubjects = req.body
      .map((changeset) => changeset.inserts)
      .flat()
      .filter((insert) => insert.predicate.value === ns.adms`status`.value)
      .filter((insert) => insert.object.value === cts.STATUS_SCHEDULED.value)
      .map((insert) => namedNode(insert.subject.value));
    if (!taskSubjects.length) {
      console.log(
        'Delta did not contain potential tasks that are interesting, awaiting the next batch!'
      );
    }
    for (const subject of taskSubjects) await processTask(subject);
  } catch (e) {
    console.error(
      'Something unexpected went wrong while handling delta task!',
      e
    );
  }
});

/**
 * Check if the given term is a task, load details from the task and execute
 * the correct pipeline for this task.
 */
async function processTask(term) {
  if (await tsk.isTask(term)) {
    const task = await tsk.loadTask(term);
    switch (task.operation.value) {
      case cts.TASK_HARVESTING_MIRRORING.value:
        await runMirrorPipeline(task);
        break;
      case cts.TASK_HARVESTING_ADD_UUIDS.value:
        await runAddUUIDs(task);
        break;
      case cts.TASK_PUBLISH_HARVESTED_TRIPLES.value:
        await runPublishPipeline(task, false);
        break;
      case cts.TASK_PUBLISH_HARVESTED_TRIPLES_WITH_DELETES.value:
        await runPublishPipeline(task, true);
        break;
      case cts.TASK_EXECUTE_DIFF_DELETES.value:
        await runExecuteDiffDeletesPipeline(task);
        break;
    }
  }
}

app.use(errorHandler);
