#include <Python.h>
#include <stdio.h>
#include <unistd.h>
#include <hbase/hbase.h>
#include <pthread.h>
#include <string.h>
#include <vector>


#if defined( WIN64 ) || defined( _WIN64 ) || defined( __WIN64__ ) || defined(_WIN32)
#define __WINDOWS__
#endif

#define CHECK_RC_RETURN(rc)          \
    do {                               \
        if (rc) {                        \
            printf("%s:%d Call failed: %d\n", __PRETTY_FUNCTION__, __LINE__, rc); \
        }                                \
    } while (0);







static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
// wtf is this shared??
static uint64_t count = 0;
static bool clientDestroyed;

static void cl_dsc_cb(int32_t err, hb_client_t client, void *extra) {
    clientDestroyed = true;
}



static const char *cldbs = "hdnprd-c01-r03-01:7222,hdnprd-c01-r04-01:7222,hdnprd-c01-r05-01:7222";
static const char *tableName = "/app/SubscriptionBillingPlatform/testInteractive";

/*
static const char *family1 = "Id";
static const char *col1_1  = "I";
static const char *family2 = "Name";
static const char *col2_1  = "First";
static const char *col2_2  = "Last";
static const char *family3 = "Address";
static const char *col3_1  = "City";
*/
static char *hbase_fqcolumn(char *family, char *column) {
    // +1 for null terminator, +1 for colon
    char *fq = (char *) malloc(1 + 1 + strlen(family) + strlen(column));
    strcpy(fq, family);
    fq[strlen(family)] = ':';
    fq[strlen(family) + 1] = '\0';
    // strcat will replace the last null terminator before writing, then add a null terminator
    strcat(fq, column);
    return fq;
}

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
    PyObject *ret;
    PyObject *rets;
};

struct Connection {
    hb_connection_t *conn;
    hb_client_t *client;
    RowBuffer *rowBuf;

    Connection() {
        rowBuf = new RowBuffer();
        int err = 0;
        err = hb_connection_create(cldbs, NULL, &conn);
        CHECK_RC_RETURN(err);

        err = hb_client_create(conn, &client);
        CHECK_RC_RETURN(err);
    }

    ~Connection() {
        hb_client_destroy(client, cl_dsc_cb, rowBuf);
        hb_connection_destroy(conn);
    }
}

/*
static void read_result(hb_result_t result) {
    if (!result) {
        return;
    }
  
    size_t cellCount = 0;
    hb_result_get_cell_count(result, &cellCount);

    for (size_t i = 0; i < cellCount; ++i) {
        const hb_cell_t *cell;
        hb_result_get_cell_at(result, i, &cell);
        printf("%s:%s = %s\t", cell->family, cell->qualifier, cell->value);
    }

    if (cellCount == 0) {
        printf("----- NO CELLS -----");
    }
}
*/

static PyObject *read_result(hb_result_t result, PyObject *dict) {
    if (!result) {
        Py_RETURN_NONE;
    }

    size_t cellCount = 0;
    hb_result_get_cell_count(result, &cellCount);

    // Probably take this as a parameter
    //PyObject *dict = PyDict_New();

    for (size_t i = 0; i < cellCount; ++i) {
        const hb_cell_t *cell;
        hb_result_get_cell_at(result, i, &cell);
        if (dict) {
            PyDict_SetItem(dict, Py_BuildValue("s", hbase_fqcolumn((char *)cell->family, (char *)cell->qualifier)), Py_BuildValue("s",(char *)cell->value));
        }
        printf("%s:%s = %s\t", cell->family, cell->qualifier, cell->value);
    }

    if (cellCount == 0) {
        printf("----- NO CELLS -----");
    }
    return dict;
}





static void get_send_cb(int32_t err, hb_client_t client, hb_get_t get, hb_result_t result, void *extra) {

    if (result) {
        const byte_t *key;
        size_t keyLen;
        hb_result_get_key(result, &key, &keyLen);
        printf("Row: %s\t", (char *)key);
        PyObject *dict = PyDict_New();
        read_result(result, dict);
        printf("\n");
        RowBuffer *rowBuf = (RowBuffer *)extra;
        printf("In extra, rowkey is %s\n", rowBuf->allocedBufs.back());
        rowBuf->ret = dict;
        hb_result_destroy(result);
    } else {
        return;
    }

    count = 1;
    hb_get_destroy(get);

    /*
    if (extra) {
        RowBuffer *rowBuf = (RowBuffer *)extra;
        printf("In extra, rowkey is %s\n", rowBuf->allocedBufs.back());
        //rowBuf->ret = dict;
        delete rowBuf;
    }
    */
}




static PyObject *test_get(char *row_key) {
    printf("Inside test_get\n");
    RowBuffer *rowBuf = new RowBuffer();
    char *rk = rowBuf->getBuffer(1024);
    //char rk[1024];
    // Both of those work^ I wonder why they use the row buffer?

    //printf("Enter key to get: ");
    //scanf("%s", rk);
    strcpy(rk, row_key);
    printf("In test_get after strcpy, rowkey is %s\n", rowBuf->allocedBufs.back());

    
    hb_connection_t conn;
    int err = 0;

    
    //if (!user) {
        err = hb_connection_create(cldbs, NULL, &conn);
        CHECK_RC_RETURN(err);
    //} else {
    //    err = hb_connection_create_as_user(cldbs, NULL, user, &conn);
    //    CHECK_RC_RETURN(err);
    //}
    
    hb_client_t client;

    err = hb_client_create(conn, &client);
    CHECK_RC_RETURN(err);
    
    hb_get_t get;
    err = hb_get_create((const byte_t *)rk, strlen(rk) + 1, &get);
    CHECK_RC_RETURN(err);

    err = hb_get_set_table(get, tableName, strlen(tableName));
    CHECK_RC_RETURN(err);


    count = 0;
    err = hb_get_send(client, get, get_send_cb, rowBuf);
    CHECK_RC_RETURN(err);

    while (count != 1) { sleep(0.1); }

    hb_client_destroy(client, cl_dsc_cb, rowBuf);

    //sleep(1);
    hb_connection_destroy(conn);
    
    printf("Done with test_get\n");
    return rowBuf->ret;
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
    read_result(result, NULL);
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


static PyObject *SpamError;

// The C function always has self and args
// for Module functions, self is NULL; for a method, self is the object
static PyObject *spam_system(PyObject *self, PyObject *args)
{
    const char *command;
    int sts;
    //PyArg_ParseTuple converts the python arguments to C values
    // It returns if all arguments are valid
    if (!PyArg_ParseTuple(args, "s", &command))
        // Returning NULL throws an exception
        return NULL;
    sts = system(command);
    if (sts < 0) {
        // Note how this sets the exception, and THEN returns null!
        PyErr_SetString(SpamError, "System command failed");
        return NULL;
    }
    return PyLong_FromLong(sts);
}


static PyObject *lol(PyObject *self, PyObject *args) {
    printf("Noob\n");
    // This is how to write a void method in python
    Py_RETURN_NONE;
}

static void noob(char *row_key) {
    printf("you are a noob");
    char rk[100];
    printf("Before segmentation fault");
    strcpy(rk, row_key);
    printf("After segmentation fault");
}
static PyObject *get(PyObject *self, PyObject *args) {
    char *row_key;
    if (!PyArg_ParseTuple(args, "s", &row_key)) {
        return NULL;
    }
    printf("hai I am %s\n", row_key);
    printf("before test_get\n");
    PyObject  *lol = test_get(row_key);
    printf("done with foo\n");
    //noob(row_key);
    return lol;
}
/*
import spam
spam.put('hai', {'Name:First': 'Matthew'})
*/

void client_flush_cb(int32_t err, hb_client_t client, void *ctx) {
    printf("Client flush callback invoked: %d\n", err);
}

static void *split(char *fq, char* arr[]) {
    int i = 0;
    // Initialize family to length + null pointer - 1 for the colon
    char *family = (char *) malloc(sizeof(char) * strlen(fq));
    for (i = 0; i < strlen(fq) && fq[i] != '\0'; i++) {
        if (fq[i] != ':') {
            family[i] = fq[i];
        } else {
            break;
        }
    }
    family[i] = '\0';

    // This works with 1+ or without 1+ ...
    char *qualifier = (char *) malloc(1 + sizeof(char) * (strlen(fq) - i));
    int qualifier_index = 0;
    for (i=i + 1; i < strlen(fq) && fq[i] != '\0'; i++) {
        qualifier[qualifier_index] = fq[i];
        qualifier_index += 1;
    }
    qualifier[qualifier_index] = '\0';

    arr[0] = family;
    arr[1] = qualifier;

    printf("arr[0] is %s\n", arr[0]);
    printf("arr[1] is %s\n", arr[1]);
}

static PyObject *put(PyObject *self, PyObject *args) {
    char *row_key;
    PyObject *dict;
    // This will raise a type error if its not a dict
    if (!PyArg_ParseTuple(args, "sO!", &row_key, &PyDict_Type, &dict)) {
        return NULL;
    }
    printf("rowkey is %s\n",row_key);

    // Create put 
    hb_connection_t conn;
    int err = 0;
    err = hb_connection_create(cldbs, NULL, &conn);
    CHECK_RC_RETURN(err);
    hb_client_t client;
    err = hb_client_create(conn, &client);
    CHECK_RC_RETURN(err);


    RowBuffer *rowBuf = new RowBuffer();
    char *rk = rowBuf->getBuffer(1024);
    strcpy(rk, row_key); // Should this be snprintf ?
    hb_put_t hb_put;

    err = hb_put_create((byte_t *)rk, strlen(rk) + 1, &hb_put);
    CHECK_RC_RETURN(err);
    printf("RC for put create was %i\n", err);

    // Loop over dict, split into family and qualifier, make cell, add to put
    PyObject *fq, *value;
    Py_ssize_t pos = 0;
    hb_cell_t *cell;
    while (PyDict_Next(dict, &pos, &fq, &value)) {
        char *arr[2];
        split(PyString_AsString(fq), arr);
        printf("family is %s\n", arr[0]);
        printf("qualifier is %s\n", arr[1]);
        char *family = rowBuf->getBuffer(1024);
        char *qualifier = rowBuf->getBuffer(1024);
        char *v = rowBuf->getBuffer(1024);

        strcpy(family, arr[0]);
        strcpy(qualifier, arr[1]);
        strcpy(v, PyString_AsString(value));

        printf("family is %s\n", family);
        printf("qualifier is %s\n", qualifier);
        printf("v is %s\n", v);

        create_dummy_cell(&cell, rk, strlen(rk), family, strlen(family) + 1, qualifier, strlen(qualifier) + 1, v, strlen(v) + 1);
        err = hb_put_add_cell(hb_put, cell);
        delete cell;
        CHECK_RC_RETURN(err);
        printf("RC for put was %i\n", err);
    }

    err = hb_mutation_set_table((hb_mutation_t)hb_put, tableName, strlen(tableName));
    CHECK_RC_RETURN(err);
    printf("RC for muttaion set table was %i\n", err);

    err = hb_mutation_send(client, (hb_mutation_t)hb_put, put_cb, rowBuf);
    CHECK_RC_RETURN(err);
    printf("RC for mutation send was %i\n", err);

    count = 0;
    hb_client_flush(client, client_flush_cb, NULL);
    printf("Waiting for all callbacks to return ...\n");
    
    //uint64_t locCount;
    //do {
    //    sleep (1);
    //    pthread_mutex_lock(&mutex);
    //    locCount = count;
    //    pthread_mutex_unlock(&mutex);
    //} while (locCount < numRows);
    
    while (count != 1) { sleep(0.1); }

    //printf("Received %lu callbacks\n", numRows);

    err = hb_client_destroy(client, cl_dsc_cb, NULL);
    CHECK_RC_RETURN(err);
    printf("RC for client destroy was %i\n", err);

    err = hb_connection_destroy(conn);
    CHECK_RC_RETURN(err);

    printf("RC for connection_destroy was %i\n", err);
    
    Py_RETURN_NONE;
}

/*
import spam
spam.scan()
*/
void sn_cb(int32_t err, hb_scanner_t scan, hb_result_t *results, size_t numResults, void *extra) {
    printf("In sn_cb\n");
    if (numResults > 0) {
        printf("bnefore rowbuff = extra\n");
        RowBuffer *rowBuf = (RowBuffer *) extra;
        PyObject *dict;
        printf("befoer loop\n");
        for (uint32_t r = 0; r < numResults; ++r) {
            printf("looping\n");
            const byte_t *key;
            size_t keyLen;
            hb_result_get_key(results[r], &key, &keyLen);
            printf("Row: %s\t", (char *)key);
            dict = PyDict_New();
            printf("before reading result into dict\n");
            read_result(results[r], dict);
            printf("before append\n");
            PyList_Append(rowBuf->rets, dict);
            printf("\n");
            printf("before destroy\n");
            hb_result_destroy(results[r]);
        }

        hb_scanner_next(scan, sn_cb, rowBuf);
    } else {
        printf(" ----- NO MORE RESULTS -----\n");
        hb_scanner_destroy(scan, NULL, NULL);
        sleep(0.1);
        pthread_mutex_lock(&mutex);
        count = 1;
        pthread_mutex_unlock(&mutex);
    }
}

static PyObject *scan(PyObject *self, PyObject *args) {
    char *start = NULL;
    char *stop = NULL;

    RowBuffer *rowBuf = new RowBuffer();
    rowBuf->rets = PyList_New(0);

    if (!PyArg_ParseTuple(args, "|ss", &start, &stop)) {
        return NULL;
    }

    hb_connection_t conn;
    int err = 0;
    err = hb_connection_create(cldbs, NULL, &conn);
    CHECK_RC_RETURN(err);
    printf("RC for conn create was %i\n", err);
    hb_client_t client;
    err = hb_client_create(conn, &client);
    CHECK_RC_RETURN(err);
    printf("RC for client create was %i\n", err);

    hb_scanner_t scan;
    err = hb_scanner_create(client, &scan);
    CHECK_RC_RETURN(err);
    printf("RC for scanner create was %i\n", err);

    err = hb_scanner_set_table(scan, tableName, strlen(tableName));
    CHECK_RC_RETURN(err);
    printf("RC for set table was %i\n", err);

    err = hb_scanner_set_num_versions(scan, 1);
    CHECK_RC_RETURN(err);
    printf("RC for num versions  was %i\n", err);

    if (start) {
        // Do I need strlen + 1 ?
        err = hb_scanner_set_start_row(scan, (byte_t *) start, strlen(start));
        CHECK_RC_RETURN(err);
        printf("RC for start row  was %i\n", err);
    }
    if (stop) {
        err = hb_scanner_set_end_row(scan, (byte_t *) stop, strlen(stop));
        CHECK_RC_RETURN(err);
        printf("RC for stop row  was %i\n", err);
    }

    // Does it optimize if I set this higher?
    err = hb_scanner_set_num_max_rows(scan, 1);
    CHECK_RC_RETURN(err);
    printf("RC for set num max rows  was %i\n", err);

    count = 0;
    err = hb_scanner_next(scan, sn_cb, rowBuf);
    CHECK_RC_RETURN(err);
    printf("RC for scanner next  was %i\n", err);

    while (count == 0) { sleep(0.1); }

    err = hb_client_destroy(client, cl_dsc_cb, NULL);
    CHECK_RC_RETURN(err);
    printf("RC for client destroy  was %i\n", err);


    err = hb_connection_destroy(conn);
    CHECK_RC_RETURN(err);
    printf("RC for connection destroy  was %i\n", err);

    return rowBuf->rets;
}


static PyObject *build_int(PyObject *self, PyObject *args) {
    return Py_BuildValue("i", 123);
}

static PyObject *build_dict(PyObject *self, PyObject *args) {
    return Py_BuildValue("{s:i}", "name", 123);
}

static PyObject *add_to_dict(PyObject *self, PyObject *args) {
    PyObject *key;
    PyObject *value;
    PyObject *dict;

    if (!PyArg_ParseTuple(args, "OOO", &dict, &key, &value)) {
        return NULL;
    }

    printf("Parsed successfully\n");

    PyDict_SetItem(dict, key, value);

    Py_RETURN_NONE;
}

static PyObject *print_dict(PyObject *self, PyObject *args) {
    PyObject *dict;

    if (!PyArg_ParseTuple(args, "O", &dict)) {
        return NULL;
    }

    PyObject *key, *value;
    Py_ssize_t pos = 0;

    while (PyDict_Next(dict, &pos, &key, &value)) {
        //PyString_AsString converts a PyObject to char * (and assumes it is actually a char * not some other data type)

        printf("key is %s\n", PyString_AsString(key));
        printf("value is %s\n", PyString_AsString(value));
    }

    Py_RETURN_NONE;

}

static PyObject *build_list(PyObject *self, PyObject *args) {
    PyObject *list = PyList_New(0);
    PyList_Append(list, Py_BuildValue("s", "hai"));
    return list;
}

static PyObject *super_dict(PyObject *self, PyObject *args) {
    char *f1;
    char *k1;
    char *v1;
    char *f2;
    char *k2;
    char *v2;

    if (!PyArg_ParseTuple(args, "ssssss", &f1, &k1, &v1, &f2, &k2, &v2)) {
        return NULL;
    }
    printf("f1 is %s\n", f1);
    printf("k1 is %s\n", k1);
    printf("v1 is %s\n", v1);
    printf("f2 is %s\n", f2);
    printf("k2 is %s\n", k2);
    printf("v2 is %s\n", v2);
    /*
    char *first = (char *) malloc(1 + 1 + strlen(f1) + strlen(f2));
    strcpy(first, f1);
    first[strlen(f1)] = ':';
    strcat(first, k1);
    */

    // somehow take args as a tuple
    PyObject *dict = PyDict_New();

    char *first = hbase_fqcolumn(f1, k1);
    char *second = hbase_fqcolumn(f2, k2);

    printf("First is %s\n", first);
    printf("Second is %s\n", second);

    PyDict_SetItem(dict, Py_BuildValue("s", first), Py_BuildValue("s", v1));
    PyDict_SetItem(dict, Py_BuildValue("s", second), Py_BuildValue("s", v2));

    return dict;
}





/*
lol = spam.build_dict()
print lol
spam.add_to_dict(lol, 'hai', 'bai')

lol = spam.


import spam
spam.super_dict('f', 'k1', 'v1', 'f2', 'k2', 'v2')

*/
/*
static PyObject *foo(PyObject *self, PyObject *args) {
    int lol = test_get(NULL);
    Py_RETURN_NONE;
}
*/

static PyMethodDef SpamMethods[] = {
    {"system",  spam_system, METH_VARARGS, "Execute a shell command."},
    {"lol", lol, METH_VARARGS, "your a lol"},
    {"get", get, METH_VARARGS, "gets a row given a rowkey"},
    {"put", put, METH_VARARGS, "puts a row and dict"},
    {"scan", scan, METH_VARARGS, "scans"},
    {"build_int", build_int, METH_VARARGS, "build an int"},
    {"build_dict", build_dict, METH_VARARGS, "build a dict"},
    {"add_to_dict", add_to_dict, METH_VARARGS, "add to dict"},
    {"super_dict", super_dict, METH_VARARGS, "super dict"},
    {"print_dict", print_dict, METH_VARARGS, "print dict"},
    {"build_list", build_list, METH_VARARGS, "build list"},
    {NULL, NULL, 0, NULL}       
};


PyMODINIT_FUNC
initspam(void)
{
    PyObject *m;

    m = Py_InitModule("spam", SpamMethods);
    if (m == NULL)
        return;

    SpamError = PyErr_NewException("spam.error", NULL, NULL);
    Py_INCREF(SpamError);
    PyModule_AddObject(m, "error", SpamError);
}

int
main(int argc, char *argv[])
{
    
    Py_SetProgramName(argv[0]);

    
    Py_Initialize();

    
    initspam();
}
