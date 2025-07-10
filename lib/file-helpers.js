import { uuid } from 'mu';
import { updateSudo } from '@lblod/mu-auth-sudo';
import { SPARQL_PREFIXES } from '../constants';
import { DataFactory } from 'n3';
import { termToString } from 'rdf-string-ttl';
import fs from 'fs/promises';
import { BASES as bs } from '../constants';
import { HIGH_LOAD_DATABASE_ENDPOINT } from '../constants';
import { NAMESPACES as ns } from '../constants';
const { literal, namedNode } = DataFactory;
const connectionOptions = {
  sparqlEndpoint: HIGH_LOAD_DATABASE_ENDPOINT,
  mayRetry: true,
};
export async function makeEmptyFile(path) {
  const handle = await fs.open(path, 'w');
  await handle.close();
}
export async function appendTempFile(content, path) {
  try {
    await fs.appendFile(path, content, 'utf-8');
  } catch (e) {
    console.log(`Failed to append TTL to file <${path}>.`);
    throw e;
  }
}

/**
 * Write the given TTL content to a file and insert the logical and physical
 * counterparts in the triplestore.
 *
 * @public
 * @async
 * @function
 * @param {NamedNode} graph - Represents the graph in which to insert the data.
 * @param {String} content - This is the TTL formatted content you want to
 * write to a file.
 * @param {String} logicalFileName - The filename for the logical file. This
 * will only be used in the triplestore. The filename on disk will be a random
 * UUID (as per usual).
 * @returns {NamedNode} Representing the logical file written to the
 * triplestore.
 */
export async function writeTtlFile(
  graph,
  tempFile,
  logicalFileName,
  derivedFrom,
) {
  const phyId = literal(uuid());
  const phyFilename = literal(`${phyId.value}.ttl`);
  const path = `/share/${phyFilename.value}`;
  const physicalFile = namedNode(path.replace('/share/', 'share://'));
  const loId = literal(uuid());
  const logicalFile = bs.files(loId.value);
  const now = literal(new Date().toISOString(), ns.xsd`dateTime`);

  try {
    await fs.rename(tempFile, path, 'utf-8');
  } catch (e) {
    console.log(`Failed to write TTL to file <${physicalFile.value}>.`);
    throw e;
  }

  try {
    const stats = await fs.stat(path);
    const fileSize = literal(stats.size, ns.xsd`integer`);

    await updateSudo(
      `
      ${SPARQL_PREFIXES}
      INSERT DATA {
        GRAPH ${termToString(graph)} {
          ${termToString(physicalFile)}
            a nfo:FileDataObject ;
            nie:dataSource ${termToString(logicalFile)} ;
            mu:uuid ${termToString(phyId)} ;
            nfo:fileName ${termToString(phyFilename)} ;
            dct:creator <http://lblod.data.gift/services/harvesting-import-service> ;
            dct:created ${termToString(now)} ;
            dct:modified ${termToString(now)} ;
            dct:format "text/turtle" ;
            nfo:fileSize ${termToString(fileSize)} ;
            dbpedia:fileExtension "ttl" .
          ${termToString(logicalFile)}
            a nfo:FileDataObject ;
            mu:uuid ${termToString(loId)} ;
            nfo:fileName ${termToString(literal(logicalFileName))} ;
            prov:wasDerivedFrom ${termToString(derivedFrom)};
            dct:creator <http://lblod.data.gift/services/harvesting-import-service> ;
            dct:created ${termToString(now)} ;
            dct:modified ${termToString(now)} ;
            dct:format "text/turtle" ;
            nfo:fileSize ${termToString(fileSize)} ;
            dbpedia:fileExtension "ttl" .
        }
      }`,
      {},
      connectionOptions,
    );
  } catch (e) {
    console.log(
      `Failed to write TTL resource <${logicalFile.value}> to triplestore.`,
    );
    throw e;
  }
  return logicalFile;
}
