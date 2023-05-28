/*
 * pq-ext-adapter.c
 *    Shim code to wrap client functions in libpq with "encryption aware variants"
 *
 * The functions call functions in Rust without exposing any libpq internals.
 * For example, PGresult is not mirrored in the Rust code and instead an intermediate
 * type (MappablePgResult) is used. This keeps the adapter code simple and makes any modifications
 * required for subsequent Postgres versions either unnecessary or trivial.
*/

#include <errno.h>
#include <stdio.h>
#include "pq-ext.h"
#include "pq-ext-v2.h"
#include "postgres_fe.h"
#include "libpq-int.h"

// The renamed original functions
// See https://www.postgresql.org/docs/15/libpq-async.html
int PQsendQuery_adaptee(PGconn *conn, const char *query);
int PQsendQuery_adaptee(PGconn *conn, const char *query);
int PQsendQueryParams_adaptee(PGconn *conn,
          const char *command,
          int nParams,
          const Oid *paramTypes,
          const char *const *paramValues,
          const int *paramLengths,
          const int *paramFormats,
          int resultFormat);
int PQsendQueryPrepared_adaptee(PGconn *conn,
					const char *stmtName,
					int nParams,
					const char *const *paramValues,
					const int *paramLengths,
					const int *paramFormats,
					int resultFormat);
int PQsendPrepare_adaptee(PGconn *conn,
			  const char *stmtName, const char *query,
			  int nParams, const Oid *paramTypes);

PGresult *PQgetResult_adaptee(PGconn *conn);
// TODO: Do this for all 6 variants of connection
PGconn *PQconnectStart_adaptee(const char *conninfo);
PGconn *PQconnectdbParams_adaptee(const char *const *keywords, const char *const *values, int expand_dbname);
PGconn *PQconnectdb_adaptee(const char *conninfo);

void PQfinish_adaptee(PGconn *conn);
void PQexecVoid(PGconn *conn, const char *query);

// Internal helper - TODO: collapse this into PQgetResult
static PGresult *CSmapResult(PQEXTDriver *driver, PGresult *result);
static PGresult *CSmapResultV2(PGconn *conn, PQEXTDriver *driver, PGresult *result);

PGconn *PQconnectdbParams(const char *const *keywords, const char *const *values, int expand_dbname)
{
  PGconn *conn = PQconnectdbParams_adaptee(keywords, values, expand_dbname);
  if (conn && conn->status != CONNECTION_BAD) {
    // Call Rust init state function
    conn->pgExtState = PQEXTinit(conn->dbName);
  }

  return conn;
}

PGconn *PQconnectdb(const char *conninfo) {
  PGconn *conn = PQconnectdb_adaptee(conninfo);

  if (conn && conn->status != CONNECTION_BAD) {
    // Call Rust init state function
    conn->pgExtState = PQEXTinit(conn->dbName);
  }

  return conn;
}

PGconn *PQconnectStart(const char *conninfo) {
  PGconn *conn = PQconnectStart_adaptee(conninfo);
  if (conn && conn->status != CONNECTION_BAD) {
    // Call Rust init state function
    conn->pgExtState = PQEXTinit(conn->dbName);
  }

  return conn;
}

void PQfinish(PGconn *conn) {
  if (conn && conn->pgExtState) {
    PQEXTfree((PQEXTDriver *)conn->pgExtState);
  }

  PQfinish_adaptee(conn);
}

int PQsendQuery(PGconn *conn, const char *query)
{
  // TODO: Handle if driver is NULL
  PQEXTDriver *driver = (PQEXTDriver *)conn->pgExtState;
  // Note that PQEXTmapQuery will check for NULL state
  return PQEXTmapQuery(query, conn, driver, PQsendQuery_adaptee, PQexecVoid);
}

int PQsendQueryPrepared(PGconn *conn,
                                const char *stmtName,
                                int nParams,
                                const char *const *paramValues,
                                const int *paramLengths,
                                const int *paramFormats,
                                int resultFormat) {
  if (conn && conn->pgExtState) {
    PQEXTDriver *driver = (PQEXTDriver *)conn->pgExtState;

    return PQEXTsendQueryPrepared(
        driver,
        conn,
        stmtName,
        nParams,
        paramValues,
        paramLengths,
        paramFormats,
        resultFormat,
        PQsendQueryPrepared_adaptee,
        PQexecVoid
    );
  } else {
    PQEXTmsgError("PQsendQueryPrepared: driver initialisation failed which may be due to misconfiguration. Learn more in the CipherStash docs: https://docs.cipherstash.com");

    return PQsendQueryPrepared_adaptee(
        conn,
        stmtName,
        nParams,
        paramValues,
        paramLengths,
        paramFormats,
        resultFormat
    );
  }
}

int PQsendPrepare(PGconn *conn,
			  const char *stmtName, const char *query,
			  int nParams, const Oid *paramTypes) {
  if (conn && conn->pgExtState) {
    PQEXTDriver *driver = (PQEXTDriver *)conn->pgExtState;

    return PQEXTsendPrepare(
        driver,
        conn,
        stmtName,
        query,
        nParams,
        paramTypes,
        PQsendPrepare_adaptee,
        PQexecVoid
    );
  } else {
    PQEXTmsgError("PQsendPrepare: driver initialisation failed which may be due to misconfiguration. Learn more in the CipherStash docs: https://docs.cipherstash.com");

    return PQsendPrepare_adaptee(
        conn,
        stmtName,
        query,
        nParams,
        paramTypes
    );
  }
}

int PQsendQueryParams(
  PGconn *conn,
  const char *command,
  int nParams,
  const Oid *paramTypes,
  const char *const *paramValues,
  const int *paramLengths,
  const int *paramFormats,
  int resultFormat)
{
  if (conn && conn->pgExtState) {
    PQEXTDriver *driver = (PQEXTDriver *)conn->pgExtState;

    return PQEXTmapQueryParams(
      driver,
      conn,
      command,
      nParams,
      paramTypes,
      paramValues,
      paramLengths,
      paramFormats,
      resultFormat,
      PQsendQueryParams_adaptee,
      PQexecVoid
    );
  } else {
    PQEXTmsgError("PQsendQueryParams: driver initialisation failed which may be due to misconfiguration. Learn more in the CipherStash docs: https://docs.cipherstash.com");

    return PQsendQueryParams_adaptee(
      conn,
      command,
      nParams,
      paramTypes,
      paramValues,
      paramLengths,
      paramFormats,
      resultFormat
    );
  }
}

PGresult *PQgetResult(PGconn *conn)
{
  // Get the query result
  PGresult *res = PQgetResult_adaptee(conn);

  if (conn && conn-> pgExtState) {
    PQEXTDriver *driver = (PQEXTDriver *)conn->pgExtState;

    if (res == NULL) {
       // If the query result was null it means all results have been returned.
       // The cache is no longer needed so clear it!
       if (!PQEXTclearValuesCache(driver)) {
         PQEXTmsgError("PQgetResult: failed to clear results cache");
       }

       return NULL;
    }

    return CSmapResultV2(conn, driver, res);
  } else {
    fprintf(stderr, "Warning: Protect Driver not initialized!\n");
    return res;
  }
}

/*
 * Maps all tuples in the result by first allocating an array of intermediate
 * PQEXTmappablePGResult structs which contain only length and a pointer to the data
 * for each tuple field. This avoids the need for Rust to understand the entire
 * PGresult struct which is chonky and is different between some versions of Postgres.
 *
 * Data pointers point only to memory that was allocated by libpq and must not be
 * free'd here or in Rust. Mapped values are copied into the data pointer.
 * This approach works because plaintexts will *always* be smaller than the
 * ciphertexts so we have enough memory already allocated.
 *
 * Once mapped, the result values must have their lengths updated which requires
 * one more full iteration through the resulst.
*/
static PGresult *CSmapResult(PQEXTDriver *driver, PGresult *result)
{
  if (!result) return NULL;

  // Map the result set to the external library
  int num_rows = PQntuples(result);
  int num_cols = PQnfields(result);

  PQEXTMappablePGResult *to_map = malloc(num_rows * num_cols * sizeof(PQEXTMappablePGResult));

  if (!to_map) {
    if (errno == ENOMEM) {
      fprintf(stderr, "FATAL: Unable to allocate memory while mapping results\n");
    }
    return NULL;
  }

  int cell_count = 0;
  // TODO: Will this approach work in singlerow mode?
  for (int col = 0; col < num_cols; col++) {
    // TODO: Could we Only try to decrypt bytea fields?
    //if (PQftype(result, col) == 17) { // BYTEAOID
      for (int row = 0; row < num_rows; row++) {
        if (PQgetisnull(result, row, col)) {
          to_map[cell_count].data = NULL;
        } else {
          to_map[cell_count].data = (unsigned char *)result->tuples[row][col].value;
        }
        to_map[cell_count].len = PQgetlength(result, row, col);
        cell_count++;
      //}
    }
  }

  if (PQEXTmapResult(driver, to_map, cell_count)) {
    cell_count = 0;
    // Set the revised value lengths
    for (int col = 0; col < num_cols; col++) {
        for (int row = 0; row < num_rows; row++) {
          if (!PQgetisnull(result, row, col)) {
            result->tuples[row][col].len = to_map[cell_count].len;
          }
          cell_count++;
      }
    }
  }

  free(to_map);
  return result;
}


// V2 FTW
static PGresult *CSmapResultV2(PGconn *conn, PQEXTDriver *driver, PGresult *result)
{
  if (!result) return NULL;

  // Map the result set to the external library
  int num_rows = PQntuples(result);
  int num_cols = PQnfields(result);

  PQExtPgResult_t results_to_map = pqext_pgresult_new(num_cols);

  for (int row = 0; row < num_rows; row++) {
    for (int col = 0; col < num_cols; col++) {
      if (PQgetisnull(result, row, col)) {
        PQExtPGValue_t item = pqext_pgvalue_new_null();
        pqext_pgresult_push(&results_to_map, item);
      } else {
        uint8_t *val = (uint8_t *)PQgetvalue(result, row, col);
        PQExtPGValue_t item = pqext_pgvalue_new(val);
        pqext_pgresult_push(&results_to_map, item);
      }
    }
  }

  PQExtPgResult_t results_mapped;
  results_mapped = pqext_map_result_v2(driver, results_to_map);
  int new_num_cols = results_mapped.col_num;

  if (new_num_cols) {
    PGresult   *new_result;
    new_result = PQmakeEmptyPGresult(conn, result->resultStatus);
    PQsetResultAttrs(new_result, new_num_cols, result->attDescs);

    // Assign value from each cell
    for (int row = 0; row < num_rows; row++) {
      for (int col = 0; col < new_num_cols; col++) {
        int idx = new_num_cols * row + col;
        if (!pqext_pgvalue_isnull(&results_mapped.values.ptr[idx])) {
          PQsetvalue(new_result, row, col, (char *)results_mapped.values.ptr[idx].data.ptr, results_mapped.values.ptr[idx].data.len);
        }
      }
    }

    // free(to_map);
    // free(result);
    // printf("we are here -\n");
    return new_result;
  } else {
    return result;
  }
}
