#include <stdio.h>
#include <unistd.h>
#include <hbase/hbase.h>
#include <pthread.h>
#include <string.h>
#include <vector>

//#if defined(_WIN32) || defined(_WIN64)
#if defined( WIN64 ) || defined( _WIN64 ) || defined( __WIN64__ ) || defined(_WIN32)
#define __WINDOWS__
#endif

#define CHECK_RC_RETURN(rc)          \
  do {                               \
    if (rc) {                        \
      printf("%s:%d Call failed: %d\n", __PRETTY_FUNCTION__, __LINE__, rc); \
      return rc;                     \
    }                                \
  } while (0);

char cldbs[1024] = {0};
const char *tableName = "/app/SubscriptionBillingPlatform/testInteractive";
const char *family1 = "Id";
const char *col1_1  = "I";
const char *family2 = "Name";
const char *col2_1  = "First";
const char *col2_2  = "Last";
const char *family3 = "Address";
const char *col3_1  = "City";

struct RowBuffer {
  std::vector<char *> allocedBufs;

  RowBuffer() {
    allocedBufs.clear();
  }

  ~RowBuffer() {
    while (allocedBufs.size() > 0) {
      char *buf = allocedBufs.back();
      allocedBufs.pop_back();
      delete [] buf;
    }
  }

  char *getBuffer(uint32_t size) {
    char *newAlloc = new char[size];
    allocedBufs.push_back(newAlloc);
    return newAlloc;
  }
};

void read_result(hb_result_t result)
{
  if (!result) {
    return;
  }
  
  size_t cellCount = 0;
  hb_result_get_cell_count(result, &cellCount);

  // Getting all cells
  for (size_t i = 0; i < cellCount; ++i) {
    const hb_cell_t *cell;
    hb_result_get_cell_at(result, i, &cell);
    printf("%s:%s = %s\t", cell->family, cell->qualifier, cell->value);
  }

  if (cellCount == 0) {
    printf("----- NO CELLS -----");
  }
}

pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
uint64_t count = 0;
bool clientDestroyed;

void cl_dsc_cb(int32_t err, hb_client_t client, void *extra)
{
//  printf("  -> Client disconnection callback called %p\n", extra);
  clientDestroyed = true;
}

void put_cb(int err, hb_client_t client, hb_mutation_t mutation,
            hb_result_t result, void *extra)
{
  if (err != 0) {
    printf("PUT CALLBACK called err = %d\n", err);
  }

  hb_mutation_destroy(mutation);
  pthread_mutex_lock(&mutex);
  count ++;
  pthread_mutex_unlock(&mutex);
  if (result) {
    const byte_t *key;
    size_t keyLen;
    hb_result_get_key(result, &key, &keyLen);
    printf("Row: %s\t", (char *)key);
    read_result(result);
    printf("\n");
    hb_result_destroy(result);
  }

  if (extra) {
    RowBuffer *rowBuf = (RowBuffer *)extra;
    delete rowBuf;
  }
}

void create_dummy_cell(hb_cell_t **cell,
                      const char *r, size_t rLen,
                      const char *f, size_t fLen,
                      const char *q, size_t qLen,
                      const char *v, size_t vLen)
{
  hb_cell_t *cellPtr = new hb_cell_t();

  cellPtr->row = (byte_t *)r;
  cellPtr->row_len = rLen;

  cellPtr->family = (byte_t *)f;
  cellPtr->family_len = fLen;

  cellPtr->qualifier = (byte_t *)q;
  cellPtr->qualifier_len = qLen;

  cellPtr->value = (byte_t *)v;
  cellPtr->value_len = vLen;

  *cell = cellPtr;
}

int test_put(hb_client_t client, int id)
{
  RowBuffer *rowBuf = new RowBuffer();
  hb_put_t put;
  int err = 0;

  char *buffer = rowBuf->getBuffer(1024);
  char *i      = rowBuf->getBuffer(1024);
  char *first  = rowBuf->getBuffer(1024);
  char *last   = rowBuf->getBuffer(1024);
  char *city   = rowBuf->getBuffer(1024);

  snprintf(buffer, 99, "row-%03d", id);
  snprintf(i, 99, "id-%03d", id);
  snprintf(first, 99, "first-%03d", id);
  snprintf(last, 99, "last-%03d", id);
  snprintf(city, 99, "city-%03d", id);

  err = hb_put_create((byte_t *)buffer, strlen(buffer) + 1, &put);
  CHECK_RC_RETURN(err);

  hb_cell_t *cell;
  create_dummy_cell(&cell, buffer, 1024, family1, strlen(family1) + 1, col1_1, strlen(col1_1) + 1, i, strlen(i) + 1);
  err = hb_put_add_cell(put, cell);
  delete cell;
  CHECK_RC_RETURN(err);

  create_dummy_cell(&cell, buffer, 1024, family2, strlen(family2) + 1, col2_1, strlen(col2_1) + 1, first, strlen(first) + 1);
  err = hb_put_add_cell(put, cell);
  delete cell;
  CHECK_RC_RETURN(err);

  create_dummy_cell(&cell, buffer, 1024, family2, strlen(family2) + 1, col2_2, strlen(col2_2) + 1, last, strlen(last) + 1);
  err = hb_put_add_cell(put, cell);
  delete cell;
  CHECK_RC_RETURN(err);

  create_dummy_cell(&cell, buffer, 1024, family3, strlen(family3) + 1, col3_1, strlen(col3_1) + 1, city, strlen(city) + 1);
  err = hb_put_add_cell(put, cell);
  delete cell;
  CHECK_RC_RETURN(err);

  err = hb_mutation_set_table((hb_mutation_t)put, tableName, strlen(tableName));
  CHECK_RC_RETURN(err);

  printf("PUT SEND[row: %s] [%s:%s = %s] [%s:%s = %s] [%s:%s = %s] [%s:%s = %s]\n",
         buffer, family1, col1_1, i, family2, col2_1, first, family2, col2_2, last, family3, col3_1, city);
  err = hb_mutation_send(client, (hb_mutation_t)put, put_cb, rowBuf);
  CHECK_RC_RETURN(err);
  return err;
}

void admin_dc_cb(int32_t err, hb_admin_t admin, void *extra)
{
  printf("admin_dc_cb: err = %d\n", err);
}

int test_admin(hb_connection_t conn)
{
  hb_admin_t admin;
  int e = hb_admin_create(conn, &admin);
  CHECK_RC_RETURN(e);

  hb_columndesc families[3];
  e = hb_coldesc_create((byte_t *)family1, strlen(family1) + 1, &families[0]);
  CHECK_RC_RETURN(e);

  e = hb_coldesc_create((byte_t *)family2, strlen(family2) + 1, &families[1]);
  CHECK_RC_RETURN(e);

  e = hb_coldesc_create((byte_t *)family3, strlen(family3) + 1, &families[2]);
  CHECK_RC_RETURN(e);

  e = hb_admin_table_exists(admin, NULL, tableName);
  if (!e) {
    printf("Table %s exists\n", tableName);
    e = hb_admin_table_delete(admin, NULL, tableName);
    CHECK_RC_RETURN(e);
  } else {
    printf("Table %s does not exist: %d\n", tableName, e);
  }

  printf("Creating table %s ...\n", tableName);
  e = hb_admin_table_create(admin, NULL, tableName, families, 3);
  CHECK_RC_RETURN(e);

  printf("Destroying admin connection ...\n");
  e = hb_admin_destroy(admin, NULL, NULL);
  CHECK_RC_RETURN(e);

}

int test_admin()
{
  hb_connection_t conn;

  int err = hb_connection_create(cldbs, NULL, &conn);
  CHECK_RC_RETURN(err);

  err = test_admin(conn);
  CHECK_RC_RETURN(err);

  err = hb_connection_destroy(conn);
}

void client_flush_cb(int32_t err, hb_client_t client, void *ctx)
{
  printf("Client flush callback invoked: %d\n", err);
}

int test_put(const char *user)
{
  hb_connection_t conn;
  int err = 0;

  if (!user) {
    err = hb_connection_create(cldbs, NULL, &conn);
    CHECK_RC_RETURN(err);
  } else {
    err = hb_connection_create_as_user(cldbs, NULL, user, &conn);
    CHECK_RC_RETURN(err);
  }

  hb_client_t client;
  err = hb_client_create(conn, &client);
  CHECK_RC_RETURN(err);

  uint64_t numRows = 0;
  printf("Number of rows to put: ");
  scanf("%lu", &numRows);

  count = 0;
  for (uint64_t i = 0; i < numRows; ++i) {
    err = test_put(client, i);
    CHECK_RC_RETURN(err);
  }

  hb_client_flush(client, client_flush_cb, NULL);
  printf("Waiting for all callbacks to return ...\n");
  uint64_t locCount;
  do {
    sleep (1);
    pthread_mutex_lock(&mutex);
    locCount = count;
    pthread_mutex_unlock(&mutex);
  } while (locCount < numRows);

  printf("Received %lu callbacks\n", numRows);

  err = hb_client_destroy(client, cl_dsc_cb, NULL);
  CHECK_RC_RETURN(err);

  sleep(1);
  err = hb_connection_destroy(conn);
  CHECK_RC_RETURN(err);
}

void sn_cb(int32_t err, hb_scanner_t scan, hb_result_t *results, size_t numResults, void *extra)
{
  if (numResults > 0) {

    for (uint32_t r = 0; r < numResults; ++r) {
      const byte_t *key;
      size_t keyLen;
      hb_result_get_key(results[r], &key, &keyLen);
      printf("Row: %s\t", (char *)key);
      read_result(results[r]);
      printf("\n");
      hb_result_destroy(results[r]);
    }

    hb_scanner_next(scan, sn_cb, NULL);
  } else {
    printf(" ----- NO MORE RESULTS -----\n");
    hb_scanner_destroy(scan, NULL, NULL);
    sleep(1);
    pthread_mutex_lock(&mutex);
    count = 1;
    pthread_mutex_unlock(&mutex);
  }
}

int test_scan(const char *user, const char *filter, uint32_t filterLen)
{
  hb_connection_t conn;
  int err = 0;

  if (!user) {
    err = hb_connection_create(cldbs, NULL, &conn);
    CHECK_RC_RETURN(err);
  } else {
    err = hb_connection_create_as_user(cldbs, NULL, user, &conn);
    CHECK_RC_RETURN(err);
  }

  hb_client_t client;
  err = hb_client_create(conn, &client);
  CHECK_RC_RETURN(err);

  hb_scanner_t scan;

  err = hb_scanner_create(client, &scan);
  CHECK_RC_RETURN(err);

  err = hb_scanner_set_table(scan, tableName, strlen(tableName));
  CHECK_RC_RETURN(err);

  err = hb_scanner_set_num_versions(scan, 2);
  CHECK_RC_RETURN(err);

  if (filter && (filterLen > 0)) {
    err = hb_scanner_set_filter(scan, (byte_t *)filter, filterLen);
    CHECK_RC_RETURN(err);
  }

  err = hb_scanner_set_num_max_rows(scan, 1);
  CHECK_RC_RETURN(err);

  count = 0;
  err = hb_scanner_next(scan, sn_cb, NULL);
  CHECK_RC_RETURN(err);

  while (count == 0) { sleep(1); }

  err = hb_client_destroy(client, cl_dsc_cb, NULL);
  CHECK_RC_RETURN(err);
  sleep(1);

  err = hb_connection_destroy(conn);
  return err;
}

void get_send_cb(int32_t err, hb_client_t client, hb_get_t get, hb_result_t result, void *extra)
{
//  printf("  get_send_cb: err=%d\n", err);
  if (result) {
    const byte_t *key;
    size_t keyLen;
    hb_result_get_key(result, &key, &keyLen);
    printf("Row: %s\t", (char *)key);
    read_result(result);
    printf("\n");
    hb_result_destroy(result);
  }

  count = 1;
  hb_get_destroy(get);

  if (extra) {
    RowBuffer *rowBuf = (RowBuffer *)extra;
    delete rowBuf;
  }
}

int test_get(const char *user)
{
  RowBuffer *rowBuf = new RowBuffer();
  char *rk = rowBuf->getBuffer(1024);

  printf("Enter key to get: ");
  scanf("%s", rk);

  hb_connection_t conn;
  int err = 0;

  if (!user) {
    err = hb_connection_create(cldbs, NULL, &conn);
    CHECK_RC_RETURN(err);
  } else {
    err = hb_connection_create_as_user(cldbs, NULL, user, &conn);
    CHECK_RC_RETURN(err);
  }

  hb_client_t client;
  err = hb_client_create(conn, &client);
  CHECK_RC_RETURN(err);

  hb_get_t get;
  err = hb_get_create((const byte_t *)rk, strlen(rk) + 1, &get);
  CHECK_RC_RETURN(err);

  err = hb_get_set_table(get, tableName, strlen(tableName));
  CHECK_RC_RETURN(err);

//  err = hb_get_set_timestamp(get, get_timestamp() - 1);

//  err = hb_get_set_num_versions(get, 2);
//  printf("get_set_num_versions: %d\n", err);

  count = 0;
  err = hb_get_send(client, get, get_send_cb, rowBuf);
  CHECK_RC_RETURN(err);

  while (count != 1) { sleep(1); }

  hb_client_destroy(client, cl_dsc_cb, rowBuf);

  sleep(1);
  hb_connection_destroy(conn);
}

int main() {
  int err = 0;

  while (true) {
    printf("Enter CLDB IPs(<ip:port>[,<ip:port]: ");
    scanf("%s", cldbs);

    hb_connection_t conn;
    err = hb_connection_create(cldbs, NULL, &conn);
    if (err) {
      printf("Could not connect to cluster %s: err=%d\n", cldbs, err);
    } else {
      hb_connection_destroy(conn);
      break;
    }
  }
  
  bool shouldExit = false;

  while (!shouldExit) {
    int option;
    char user[100] = {0};
    char filter[1000] = {0};
    int i = 0;
    char ch;

    printf("Please select from following options:\n");
    printf("1. Create table\n");
    printf("2. Puts\n");
    printf("3. Impersonated puts\n");
    printf("4. Scan\n");
    printf("5. Impersonated Scan\n");
    printf("6. Get\n");
    printf("7. Impersonated Get\n");
    printf("0. Exit\n");
    printf("Enter here: ");
    scanf("%d", &option);

    switch(option) {
      case 1:
        test_admin();
        break;
      case 2:
        test_put(NULL);
        break;
      case 3:
#ifdef __WINDOWS__
        printf("Cannot perform impersonation on windows clients.\n");
#else
        printf("Enter the user to impersonate: ");
        scanf("%s", user);
        test_put(user);
#endif
        break;
      case 4:
        getc(stdin);
        printf("Enter filter string(Press Enter for no filter): ");
        while ((ch = getc(stdin)) != '\n') {
          filter[i ++] = ch;
        }

        test_scan(NULL, filter, i);
        break;
      case 5:
#ifdef __WINDOWS__
        printf("Cannot perform impersonation on windows clients.\n");
#else
        printf("Enter the user to impersonate: ");
        scanf("%s", user);
        getc(stdin);
        printf("Enter filter string(Press Enter for no filter): ");
        while ((ch = getc(stdin)) != '\n') {
          filter[i ++] = ch;
        }

        test_scan(user, filter, i);
#endif
        break;
      case 6:
        test_get(NULL);
        break;
      case 7:
#ifdef __WINDOWS__
        printf("Cannot perform impersonation on windows clients.\n");
#else
        printf("Enter the user to impersonate: ");
        scanf("%s", user);
        test_get(user);
#endif
        break;

      case 0:
        shouldExit = true;
        break;
    }
  }

  return 0;
}
