import * as cts from './constants';
import * as tsk from './lib/task';
import * as N3 from 'n3';
import bodyParser from 'body-parser';
import { NAMESPACES as ns } from './constants';
import { app, errorHandler } from 'mu';
import { run as runMirrorPipeline } from './lib/pipeline-mirroring';
import { run as runImportPipeline } from './lib/pipeline-importing';
import { run as runAddUUIDs } from './lib/pipeline-add-uuids';
const { namedNode } = N3.DataFactory;

app.use(
  bodyParser.json({
    type: function (req) {
      return /^application\/json/.test(req.get('content-type'));
    },
  })
);

app.get('/', function (_, res) {
  res.send('Hello harvesting-import-sameas-service');
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
        'Delta dit not contain potential tasks that are interesting, awaiting the next batch!'
      );
    }

    // On all tasks in the body, load some details of the task and see if it is
    // a task that is meant to be processed by this service. Execute the
    // pipeline if so.
    for (const subject of taskSubjects) {
      if (await tsk.isTask(subject)) {
        const task = await tsk.loadTask(subject);
        switch (task.operation.value) {
          case cts.TASK_PUBLISH_HARVESTED_TRIPLES.value:
            await runImportPipeline(task);
            break;
          case cts.TASK_HARVESTING_MIRRORING.value:
            await runMirrorPipeline(task);
            break;
          case cts.TASK_HARVESTING_ADD_UUIDS.value:
            await runAddUUIDs(task);
            break;
        }
      }
    }
  } catch (e) {
    console.error(
      'Something unexpected went wrong while handling delta task!',
      e
    );
  }
});

app.use(errorHandler);
