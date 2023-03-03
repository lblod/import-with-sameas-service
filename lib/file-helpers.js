import * as mu from 'mu';
import * as mas from '@lblod/mu-auth-sudo';
import * as cts from '../constants';
import * as N3 from 'n3';
import * as rst from 'rdf-string-ttl';
import fs from 'fs/promises';
import { BASES as bs } from '../constants';
import { NAMESPACES as ns } from '../constants';
const { literal, namedNode } = N3.DataFactory;

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
export async function writeTtlFile(graph, content, logicalFileName) {
  const phyId = literal(mu.uuid());
  const phyFilename = literal(`${phyId.value}.ttl`);
  const path = `/share/${phyFilename.value}`;
  const physicalFile = namedNode(path.replace('/share/', 'share://'));
  const loId = literal(mu.uuid());
  const logicalFile = bs.files(loId.value);
  const now = literal(new Date().toISOString(), ns.xsd`dateTime`);

  try {
    await fs.writeFile(path, content, 'utf-8');
  } catch (e) {
    console.log(`Failed to write TTL to file <${physicalFile.value}>.`);
    throw e;
  }

  try {
    const stats = await fs.stat(path);
    const fileSize = literal(stats.size, ns.xsd`integer`);

    await mas.updateSudo(`
      ${cts.SPARQL_PREFIXES}
      INSERT DATA {
        GRAPH ${rst.termToString(graph)} {
          ${rst.termToString(physicalFile)}
            a nfo:FileDataObject ;
            nie:dataSource ${rst.termToString(logicalFile)} ;
            mu:uuid ${rst.termToString(phyId)} ;
            nfo:fileName ${rst.termToString(phyFilename)} ;
            dct:creator <http://lblod.data.gift/services/harvesting-import-service> ;
            dct:created ${rst.termToString(now)} ;
            dct:modified ${rst.termToString(now)} ;
            dct:format "text/turtle" ;
            nfo:fileSize ${rst.termToString(fileSize)} ;
            dbpedia:fileExtension "ttl" .
          ${rst.termToString(logicalFile)}
            a nfo:FileDataObject ;
            mu:uuid ${rst.termToString(loId)} ;
            nfo:fileName ${rst.termToString(literal(logicalFileName))} ;
            dct:creator <http://lblod.data.gift/services/harvesting-import-service> ;
            dct:created ${rst.termToString(now)} ;
            dct:modified ${rst.termToString(now)} ;
            dct:format "text/turtle" ;
            nfo:fileSize ${rst.termToString(fileSize)} ;
            dbpedia:fileExtension "ttl" .
        }
      }`);
  } catch (e) {
    console.log(
      `Failed to write TTL resource <${logicalFile.value}> to triplestore.`
    );
    throw e;
  }
  return logicalFile;
}
