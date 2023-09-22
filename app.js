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
import { run as runAddHarvestingTag } from './lib/pipeline-add-harvesting-tag';
import { Lock } from 'async-await-mutex-lock';
const { namedNode } = N3.DataFactory;

/**
 * Lock to make sure that some functions don't run at the same time. E.g. when
 * a delta is still processing, you don't want to allow the manual searching
 * and restarting as it will also pick up the delta that is already busy
 * processing.
 */
// const LOCK = new Lock(); DISABLED FOR NOW

app.use(
  bodyParser.json({
    limit: '50mb',
    type: function(req) {
      return /^application\/json/.test(req.get('content-type'));
    },
  }),
);

/**
 * Find unfinished tasks (busy or scheduled) and start them (again) from
 * scratch.
 *
 * @async
 * @function
 * @returns { undefined } Nothing
 */
async function findAndStartUnfinishedTasks() {
  try {
    const unfinishedTasks = await tsk.getUnfinishedTasks();
    console.log('unfinishedTasks: ', JSON.stringify(unfinishedTasks));
    for (const term of unfinishedTasks) await processTask(term);
  } catch (e) {
    console.error('Something went wrong while scheduling unfinished taks', e);
  }
}

/**
 * Run on startup.
 */
setTimeout(async () => {
  console.log('check if there is a task');
  await tsk.waitForDatabase();
  await findAndStartUnfinishedTasks();
}, 1000);

app.get('/', function(_, res) {
  res.send('Hello harvesting-import-sameas-service');
});

app.post('/find-and-start-unfinished-tasks', async function(req, res) {
  res
    .json({ status: 'Finding and restarting unfinished tasks' })
    .status(200)
    .end();
  // await LOCK.acquire(); DISABLED FOR NOW
  try {
    await findAndStartUnfinishedTasks();
  } finally {
    // LOCK.release(); DISABLED FOR NOW
  }
});

app.post('/force-retry-task', async function(req, res) {
  const taskUri = req.body?.uri;
  if (!taskUri)
    res.status(400).send({
      status:
        'No task URI given in the request body. Please send a JSON body with a `status` key and a task URI as value.',
    });
  res.status(200).send({ status: `Force restarting task \`${taskUri}\`` });
  // await LOCK.acquire(); DISABLED FOR NOW
  try {
    await processTask(namedNode(taskUri));
  } finally {
    //  LOCK.release(); DISABLED FOR NOW
  }
});

app.post('/delta', async function(req, res) {
  console.log('receiving something');
  try {
    // await LOCK.acquire(); DISABLE LOCK for now
    // Filter for triples in the body that are inserts about a task with a
    // status 'scheduled'.
    const taskSubjects = req.body
      .map((changeset) => changeset.inserts)
      .flat()
      //.filter((insert) => insert.predicate.value === ns.adms`status`.value)
      .filter((insert) => insert.object.value === cts.STATUS_SCHEDULED.value)
      .map((insert) => namedNode(insert.subject.value));
    if (!taskSubjects.length) {
      console.log(
        'Delta did not contain potential tasks that are interesting, awaiting the next batch!',
        JSON.stringify(req.body),
      );
    }
    for (const subject of taskSubjects) {
      console.log('processing subject ', subject);
      await processTask(subject);
    }
    return res.status(200).send().end();
  } catch (e) {
    console.error(
      'Something unexpected went wrong while handling delta task!',
      e,
    );
    return next(e);
  } finally {
    // LOCK.release(); DISABLE LOCK FOR NOW
  }
});

/**
 * Check if the given term is a task, load details from the task and execute
 * the correct pipeline for this task.
 *
 * @async
 * @function
 * @param { NamedNode } term - Represents the task that needs to be started.
 * @returns { undefined } Nothing
 */
async function processTask(term) {
  try {
    if (await tsk.isTask(term)) {
      const task = await tsk.loadTask(term);
      switch (task.operation.value) {
        case cts.TASK_HARVESTING_MIRRORING.value:
          await runMirrorPipeline(task);
          break;
        case cts.TASK_HARVESTING_ADD_UUIDS.value:
          console.log('detected uuid task');
          await runAddUUIDs(task);
          break;
        case cts.TASK_HARVESTING_ADD_TAG.value:
          await runAddHarvestingTag(task);
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
        default:
          console.log('could not process task ', JSON.stringify(task));
      }
    }
  } catch (e) {
    console.error(`Something went wrong while processing task: ${term}`, e);
  }
}

app.use(errorHandler);
