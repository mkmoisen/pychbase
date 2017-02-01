#include <Python.h>
#include "structmember.h"
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


static PyObject *SpamError;

typedef struct {
    // This is a macro, correct with no semi colon, which initializes fields to make it usable as a PyObject
    // Why not define first and last as char * ? Is there any benefit over each way?
    PyObject_HEAD
    PyObject *first;
    PyObject *last;
    int number;
    char *secret;
} Foo;

static void Foo_dealloc(Foo *self) {
    //dispose of your owned references
    //Py_XDECREF is sued because first/last could be NULL
    Py_XDECREF(self->first);
    Py_XDECREF(self->last);
    //call the class tp_free function to clean up the type itself.
    // Note how the Type is PyObject * insteaed of FooType * because the object may be a subclass
    self->ob_type->tp_free((PyObject *) self);

    // Note how there is no XDECREF on self->number
}

static PyObject *Foo_new(PyTypeObject *type, PyObject *args, PyObject *kwargs) {
    // Hm this isn't printing out?
    // Ok Foo_new isn't being called for some reason
    printf("In foo_new\n");
    Foo *self;// == NULL;
    // to_alloc allocates memory
    self = (Foo *)type->tp_alloc(type, 0);
    // One reason to implement a new method is to assure the initial values of instance variables
    // Here we are ensuring they initial values of first and last are not NULL.
    // If we don't care, we ould have used PyType_GenericNew() as the new method, which sets everything to NULL...
    if (self != NULL) {
        printf("in neww self is not null");
        self->first = PyString_FromString("");
        if (self->first == NULL) {
            Py_DECREF(self);
            return NULL;
        }

        self->last = PyString_FromString("");
        if (self->last == NULL) {
            Py_DECREF(self);
            return NULL;
        }

        self->number = 0;
    }


    // What about self->secret ?

    if (self->first == NULL) {
        printf("in new self first is null\n");
    } else {
        printf("in new self first is not null\n");
    }

    return (PyObject *) self;
}

static int Foo_init(Foo *self, PyObject *args, PyObject *kwargs) {
    //char *name;
    printf("In foo_init\n");
    PyObject *first, *last, *tmp;
    // Note how we can use &self->number, but not &self->first
    if (!PyArg_ParseTuple(args, "SSi", &first, &last, &self->number)) {
        //return NULL;
        return -1;
    }
    // What is the point of tmp?
    // The docs say we should always reassign members before decrementing their reference counts

    if (last) {
        tmp = self->last;
        Py_INCREF(last);
        self->last = last;
        Py_DECREF(tmp);
    }

    if (first) {
        tmp = self->first;
        Py_INCREF(first);
        self->first = first;
        //This was changed to DECREF from XDECREF once the get_first/last were set
        // This is because the get_first/last guarantee that it isn't null
        // but it caused a segmentation fault wtf?
        // Ok that was because the new method wasn't working bug
        Py_DECREF(tmp);
    }


    // Should I incref this?
    self->secret = "secret lol";
    printf("Finished foo_init");
    return 0;
}

/*
import spam
spam.Foo('a','b',5)
*/


// Make data available to Python
static PyMemberDef Foo_members[] = {
    //{"first", T_OBJECT_EX, offsetof(Foo, first), 0, "first name"},
    //{"last", T_OBJECT_EX, offsetof(Foo, last), 0, "last name"},
    {"number", T_INT, offsetof(Foo, number), 0, "number"},
    {NULL}
};

static PyObject *Foo_get_first(Foo *self, void *closure) {
    Py_INCREF(self->first);
    return self->first;
}

static int Foo_set_first(Foo *self, PyObject *value, void *closure) {
    printf("IN foo_set_first\n");
    if (value == NULL) {
        PyErr_SetString(PyExc_TypeError, "Cannot delete the first attribute");
        return -1;
    }

    if (!PyString_Check(value)) {
        PyErr_SetString(PyExc_TypeError, "The first attribute value must be a string");
        return -1;
    }

    Py_DECREF(self->first);
    Py_INCREF(value);
    self->first = value;
    printf("finished foo_set_first\n");
    return 0;
}

static PyObject *Foo_get_last(Foo *self, void *closure) {
    Py_INCREF(self->last);
    return self->last;
}

static int Foo_set_last(Foo *self, PyObject *value, void *closure) {
    printf("IN foo_set_last\n");
    if (value == NULL) {
        PyErr_SetString(PyExc_TypeError, "Cannot delete the last attribute");
        return -1;
    }

    if (!PyString_Check(value)) {
        PyErr_SetString(PyExc_TypeError, "The last attribute must be a string");
        return -1;
    }

    Py_DECREF(self->last);
    Py_INCREF(value);
    self->last = value;
    printf("finished foo_set_last\n");
    return 0;
}

static PyGetSetDef Foo_getseters[] = {
    {"first", (getter) Foo_get_first, (setter) Foo_set_first, "first name", NULL},
    {"last", (getter) Foo_get_last, (setter) Foo_set_last, "last name", NULL},
    {NULL}
};

static PyObject *Foo_square(Foo *self) {
    return Py_BuildValue("i", self->number * self->number);
}

static PyObject * Foo_name(Foo *self) {
    static PyObject *format = NULL;

    PyObject *args, *result;

    // We have to check for NULL, because they can be deleted, in which case they are set to NULL.
    // It would be better to prevent deletion of these attributes and to restrict the attribute values to strings.
    if (format == NULL) {
        format = PyString_FromString("%s %s");
        if (format == NULL) {
            return NULL;
        }
    }
    /*
    // These checks can be removed after adding the getter/setter that guarentees it cannot be null
    if (self->first == NULL) {
        PyErr_SetString(PyExc_AttributeError, "first");
        return NULL;
    }

    if (self->last == NULL) {
        PyErr_SetString(PyExc_AttributeError, "last");
        return NULL;
    }
    */

    args = Py_BuildValue("OO", self->first, self->last);
    if (args == NULL) {
        return NULL;
    }

    result = PyString_Format(format, args);
    // What is the difference between XDECREF and DECREF?
    // Use XDECREF if something can be null, DECREF if it is guarenteed to not be null
    Py_DECREF(args);

    return result;
}

// Make methods available
static PyMethodDef Foo_methods[] = {
    {"square", (PyCFunction) Foo_square, METH_VARARGS, "squares an int"},
    // METH_NOARGS indicates that this method should not be passed any arguments
    {"name", (PyCFunction) Foo_name, METH_NOARGS, "Returns the full name"},
    {NULL}
};

// Declare the type components
static PyTypeObject FooType = {
   PyObject_HEAD_INIT(NULL)
   0,                         /* ob_size */
   "spam.Foo",               /* tp_name */
   sizeof(Foo),         /* tp_basicsize */
   0,                         /* tp_itemsize */
   (destructor)Foo_dealloc, /* tp_dealloc */
   0,                         /* tp_print */
   0,                         /* tp_getattr */
   0,                         /* tp_setattr */
   0,                         /* tp_compare */
   0,                         /* tp_repr */
   0,                         /* tp_as_number */
   0,                         /* tp_as_sequence */
   0,                         /* tp_as_mapping */
   0,                         /* tp_hash */
   0,                         /* tp_call */
   0,                         /* tp_str */
   0,                         /* tp_getattro */
   0,                         /* tp_setattro */
   0,                         /* tp_as_buffer */
   Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /* tp_flags*/
   "Foo object",        /* tp_doc */
   0,                         /* tp_traverse */
   0,                         /* tp_clear */
   0,                         /* tp_richcompare */
   0,                         /* tp_weaklistoffset */
   0,                         /* tp_iter */
   0,                         /* tp_iternext */
   Foo_methods,         /* tp_methods */
   Foo_members,         /* tp_members */
   Foo_getseters,                         /* tp_getset */
   0,                         /* tp_base */
   0,                         /* tp_dict */
   0,                         /* tp_descr_get */
   0,                         /* tp_descr_set */
   0,                         /* tp_dictoffset */
   (initproc)Foo_init,  /* tp_init */
   0,                         /* tp_alloc */
   Foo_new,                         /* tp_new */
};










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

/*
Given a family and a qualifier, return a fully qualified column (familiy + ":" + qualifier)
*/

static char *hbase_fqcolumn(const hb_cell_t *cell) {
    if (!cell) {
        printf("cell is null\n");
        return NULL;
    }
    char *family = (char *) cell->family;
    char *qualifier = (char *) cell->qualifier;

    int family_len = cell->family_len;
    int qualifier_len = cell->qualifier_len;
    printf("family_len is %i\n", family_len);
    printf("family is %s\n", family);
    printf("qualifier_len is %i\n", qualifier_len);
    printf("qualifier is %s\n", qualifier);

    // +1 for null terminator, +1 for colon
    //TODO This one is probably correct
    char *fq = (char *) malloc(1 + 1 + family_len + qualifier_len);

    if (!fq) {
        return NULL;
    }
    strncpy(fq, family, family_len);
    printf("fq is %s\n", fq);
    fq[family_len] = ':';
    printf("fq is %s\n", fq);
    fq[family_len + 1] = '\0';
    //fq[strlen(family)] = '\0';
    printf("fq is %s\n", fq);
    // strcat will replace the last null terminator before writing, then add a null terminator
    strncat(fq, qualifier, qualifier_len);
    printf("fq is %s\n", fq);
    return fq;
}


struct RowBuffer {
    std::vector<char *> allocedBufs;

    RowBuffer() {
        allocedBufs.clear();
    }

    ~RowBuffer() {
        //printf("RowBuffer Destructor\n");
        while (allocedBufs.size() > 0) {
            char *buf = allocedBufs.back();
            allocedBufs.pop_back();
            delete [] buf;
            // It looks like these do the same thing? What is the difference between free and delete [] here?
            //free(buf);
        }
    }

    char *getBuffer(uint32_t size) {
        char *newAlloc = new char[size];
        allocedBufs.push_back(newAlloc);
        return newAlloc;
    }
    //PyObject *ret;
    //PyObject *rets;
};



/*
import spam
connection = spam._connection("hdnprd-c01-r03-01:7222,hdnprd-c01-r04-01:7222,hdnprd-c01-r05-01:7222")
connection.is_open()
connection.open()
connection.is_open()
connection.close()
connection.is_open()
*/



typedef struct {
    PyObject_HEAD
    PyObject *cldbs;
    // Add an is_open boolean
    bool is_open;
    hb_connection_t conn;
    hb_client_t client;
    hb_admin_t admin;
    RowBuffer *rowBuf;
} Connection;

static void Connection_dealloc(Connection *self) {
    Py_XDECREF(self->cldbs);

    //hb_admin_destroy(self->admin, admin_disconnection_callback)

    self->ob_type->tp_free((PyObject *) self);



    // I don't think I need to Py_XDECREF on conn and client?
}

// I'm going to skip Connection_new
// remember to FooType.tp_new = PyType_GenericNew;


static int Connection_init(Connection *self, PyObject *args, PyObject *kwargs) {
    PyObject *cldbs, *tmp;

    // Add an is_open boolean
    if (!PyArg_ParseTuple(args, "S", &cldbs)) {
        return -1;
    }

    if (cldbs) {
        tmp = self->cldbs;
        Py_INCREF(cldbs);
        self->cldbs = cldbs;
        Py_XDECREF(tmp);
    }
    // Todo make this optional, and then find it from /opt/mapr/conf



    return 0;
}

static PyMemberDef Connection_members[] = {
    {"cldbs", T_OBJECT_EX, offsetof(Connection, cldbs), 0, "The cldbs connection string"},
    {NULL}
};

/*
import spam
connection = spam._connection("hdnprd-c01-r03-01:7222,hdnprd-c01-r04-01:7222,hdnprd-c01-r05-01:7222")
connection.is_open()
connection.open()
connection.is_open()
connection.close()
connection.is_open()
connection.close()

connection = spam._connection("abc")
connection.open()
connection.is_open()
connection.close()
connection.cldbs = "hdnprd-c01-r03-01:7222,hdnprd-c01-r04-01:7222,hdnprd-c01-r05-01:7222"
connection.open()
connection.is_open()

table = spam._table(connection, '/app/SubscriptionBillingPlatform/testInteractive')
*/
static PyObject *Connection_open(Connection *self) {
    self->rowBuf = new RowBuffer();
    if (!self->is_open) {
        int err = 0;
        err = hb_connection_create(PyString_AsString(self->cldbs), NULL, &self->conn);
        //printf("err hb_connection_create %i\n", err);
        if (err != 0) {
            PyErr_SetString(PyExc_ValueError, "Could not connect using CLDBS");
            return NULL;
        }

        err = hb_client_create(self->conn, &self->client);
        //printf("err hb_client_create %i\n", err);
        if (err != 0) {
            PyErr_SetString(PyExc_ValueError, "Could not create client from connection");
            return NULL;
        }

        //Add an is_open boolean
        self->is_open = true;

        err = hb_admin_create(self->conn, &self->admin);
        CHECK_RC_RETURN(err);
        if (err != 0) {
            PyErr_SetString(PyExc_ValueError, "Could not create admin from connection");
            return NULL;
        }

    }
    Py_RETURN_NONE;

}

static void cl_dsc_cb(int32_t err, hb_client_t client, void *connection) {
    // Perhaps I could add a is_client_open boolean to connection ?
}

static PyObject *Connection_close(Connection *self) {
    if (self->is_open) {
        hb_client_destroy(self->client, cl_dsc_cb, self);
        hb_connection_destroy(self->conn);
        self->is_open = false;
    }
    Py_RETURN_NONE;
}

static PyObject *Connection_is_open(Connection *self) {
    if (self->is_open) {
        return Py_True;
    }
    return Py_False;
}


/*
import spam
connection = spam._connection("hdnprd-c01-r03-01:7222,hdnprd-c01-r04-01:7222,hdnprd-c01-r05-01:7222")
connection.open()
connection.create_table("/app/SubscriptionBillingPlatform/testpymaprdb21", {'f1': {}})


connection.create_table_wtf("/app/SubscriptionBillingPlatform/testpymaprdb20", {'f1': 'a'})




connection.create_table_wtf("/app/SubscriptionBillingPlatform/testpymaprdb11", ['hello'])


*/

static PyObject *Connection_delete_table(Connection *self, PyObject *args) {
    char *table_name;
    char *name_space;
    if (!PyArg_ParseTuple(args, "s|s", &table_name, &name_space)) {
        return NULL;
    }

    if (!self->is_open) {
        Connection_open(self);
    }

    int table_name_length = strlen(table_name);
    if (table_name_length && table_name_length > 1000) {
        PyErr_SetString(PyExc_ValueError, "Table name is too long\n");
        return NULL;
    }

    int err;

    err = hb_admin_table_exists(self->admin, NULL, table_name);
    CHECK_RC_RETURN(err);

    if (err != 0) {
        // I guess I have to return -1 nothing else to cause the correct failure
        PyErr_SetString(PyExc_ValueError, "Table does not exist\n");
        //return NULL; // return err;
        return NULL;
    }

    err = hb_admin_table_delete(self->admin, name_space, table_name);

    if (err != 0) {
        PyErr_SetString(PyExc_ValueError, "Failed to admin delete create");
        return NULL;
    }

    Py_RETURN_NONE;
}

static PyObject *Connection_create_table(Connection *self, PyObject *args) {
    int err;
    char *table_name;
    PyObject *dict;
    if (!PyArg_ParseTuple(args, "sO!", &table_name, &PyDict_Type, &dict)) {
        printf("noob in parse tuple\n");
        return NULL;
    }
    printf("dict ref count is %i\n",dict->ob_refcnt);
    if (!self->is_open) {
        Connection_open(self);
    }
    int table_name_length = strlen(table_name);
    if (table_name_length && table_name_length > 1000) {
        PyErr_SetString(PyExc_ValueError, "Table name is too long\n");
        return NULL;
    }
    printf("table name is %s\n", table_name);
    printf("before admin table exists\n");
    printf("strlen(table_name) is %i\n", strlen(table_name));
    err = hb_admin_table_exists(self->admin, NULL, table_name);
    printf("after admin table exists\n");
    if (err == 0) {
        // I guess I have to return -1 nothing else to cause the correct failure
        printf("we have err %i\n", err);
        PyErr_SetString(PyExc_ValueError, "Table already exists\n");

        //return NULL; // return err;
        printf("before return\n");
        return NULL;
    }
    printf("no err\n");

    PyObject *column_family_name;
    PyObject *column_family_attributes;
    Py_ssize_t i = 0;
    //int i = 0;

    int number_of_families = PyDict_Size(dict);
    if (number_of_families < 1) {
        PyErr_SetString(PyExc_ValueError, "Need at least one column family");
        return NULL;
    }
    hb_columndesc families[number_of_families];

    int counter = 0;

    while (PyDict_Next(dict, &i, &column_family_name, &column_family_attributes)) {
        printf("looping\n");
        printf("dict ref count is %i\n",dict->ob_refcnt);
        printf("column family name is %i\n",column_family_name->ob_refcnt);
        printf("column_family_attributes is %i\n",column_family_attributes->ob_refcnt);
        printf("before asstring family name create\n");

        char *column_family_name_char = PyString_AsString(column_family_name);
        printf("column family name is %i\n",column_family_name->ob_refcnt);
        if (!column_family_name_char) {
            PyErr_SetString(PyExc_ValueError, "Out of memmory");
            return NULL;
        }

        printf("before coldesc create\n");
        err = hb_coldesc_create((byte_t *)column_family_name_char, strlen(column_family_name_char) + 1, &families[counter]);
        printf("after coldesc create\n");
        printf("column family name is %i\n",column_family_name->ob_refcnt);
        printf("column_family_attributes is %i\n",column_family_attributes->ob_refcnt);
        if (err != 0) {
            PyErr_SetString(PyExc_ValueError, "Failed to create coldesc");
            return NULL;
        }
        //printf("In loop name is %s\n", (char *)columndesc.family);

        //families[i] = columndesc;

        int is_dict = PyDict_Check(column_family_attributes);
        if (is_dict == 1) {
            printf("Its a dict lol\n");

            Py_ssize_t dict_size = PyDict_Size(column_family_attributes);
            printf("The size is %i\n", dict_size);
        } else {
            printf("Its NOT a dict lol\n");
        }

        PyObject *key, *value;
        Py_ssize_t o = 0;
        while (PyDict_Next(column_family_attributes, &o, &key, &value)) {
            printf("Looping through attrs\n");

            if (!PyString_Check(key) && !PyUnicode_Check(key)) {
                PyErr_SetString(PyExc_ValueError, "Key must be string");
                return NULL;
            }
            if (!PyInt_Check(value)) {
                PyErr_SetString(PyExc_ValueError, "Value must be int");
                return NULL;
            }
            char *key_char = PyString_AsString(key);
            printf("attribute is %s\n", key_char);
            if (!key_char) {
                PyErr_SetString(PyExc_ValueError, "Out of memmory");
                return NULL;
            }

            printf("after !key_char\n");
            if (strcmp(key_char, "max_versions") == 0) {
                printf("It's max versions\n");
                int max_versions = PyInt_AsSsize_t(value);
                printf("in max versions its %i\n", max_versions);
                // error check?
                err = hb_coldesc_set_maxversions(families[counter], max_versions);
                printf("After set max versions err is %i\n", err);
                if (err != 0) {
                    PyErr_SetString(PyExc_ValueError, "Failed to add max version to column desc");
                    return NULL;
                }
            } else if (strcmp(key_char, "min_versions") == 0) {
                int min_versions = PyInt_AsSsize_t(value);
                err = hb_coldesc_set_minversions(families[counter], min_versions);
                if (err != 0) {
                    PyErr_SetString(PyExc_ValueError, "Failed to add min version to column desc");
                    return NULL;
                }
            } else if (strcmp(key_char, "time_to_live") == 0) {
                int time_to_live = PyInt_AsSsize_t(value);
                err = hb_coldesc_set_ttl(families[counter], time_to_live);
                if (err != 0) {
                    PyErr_SetString(PyExc_ValueError, "Failed to add time to live to column desc");
                    return NULL;
                }
            } else if (strcmp(key_char, "in_memory") == 0) {
                int in_memory = PyInt_AsSsize_t(value);
                err = hb_coldesc_set_inmemory(families[counter], in_memory);
                if (err != 0) {
                    PyErr_SetString(PyExc_ValueError, "Failed to add in memory to column desc");
                    return NULL;
                }
            } else {
                PyErr_SetString(PyExc_ValueError, "Only max_versions, min_version, time_to_live, or in_memory permitted");
                return NULL;
            }

        }

        counter++;
    }

    printf("before table create\n");
    err = hb_admin_table_create(self->admin, NULL, table_name, families, number_of_families);
    printf("after table create\n");
    printf("before if\n");

    if (err != 0) {
        printf("Error != 0\n");
        if (err == 36) {
            PyErr_SetString(PyExc_ValueError, "Table name is too long\n");
        } else {
            PyErr_SetString(PyExc_ValueError, "Failed to admin table create");
        }

        printf("returning null\n");
        // Sometimes if it fails to create, the table still gets created but doesn't work?
        // Attempt to delete it
        printf("table name is %s\n", table_name);
        PyObject *table_name_obj = Py_BuildValue("(s)", table_name);
        if (table_name_obj) {
            Connection_delete_table(self, table_name_obj);
        }
        return NULL;
        // TODO check err code for column family too big?
        // TODO test for really large table name?
    }
    printf("after if not tru\n");
    Py_RETURN_NONE;
}


/*
import spam
connection = spam._connection("hdnprd-c01-r03-01:7222,hdnprd-c01-r04-01:7222,hdnprd-c01-r05-01:7222")
connection.open()
for i in range(1,20):
    try:
        connection.delete_table("/app/SubscriptionBillingPlatform/testpymaprdb{}".format(i))
    except ValueError:
        pass


*/




static PyMethodDef Connection_methods[] = {
    {"open", (PyCFunction) Connection_open, METH_NOARGS, "Opens the connection"},
    {"close", (PyCFunction) Connection_close, METH_NOARGS, "Closes the connection"},
    {"is_open", (PyCFunction) Connection_is_open, METH_NOARGS,"Checks if the connection is open"},
    {"create_table", (PyCFunction) Connection_create_table, METH_VARARGS, "Creates an HBase table"},
    {"delete_table", (PyCFunction) Connection_delete_table, METH_VARARGS, "Deletes an HBase table"},
    {NULL},
};

// Declare the type components
static PyTypeObject ConnectionType = {
   PyObject_HEAD_INIT(NULL)
   0,                         /* ob_size */
   "spam._connection",               /* tp_name */
   sizeof(Connection),         /* tp_basicsize */
   0,                         /* tp_itemsize */
   (destructor)Connection_dealloc, /* tp_dealloc */
   0,                         /* tp_print */
   0,                         /* tp_getattr */
   0,                         /* tp_setattr */
   0,                         /* tp_compare */
   0,                         /* tp_repr */
   0,                         /* tp_as_number */
   0,                         /* tp_as_sequence */
   0,                         /* tp_as_mapping */
   0,                         /* tp_hash */
   0,                         /* tp_call */
   0,                         /* tp_str */
   0,                         /* tp_getattro */
   0,                         /* tp_setattro */
   0,                         /* tp_as_buffer */
   Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /* tp_flags*/
   "Connection object",        /* tp_doc */
   0,                         /* tp_traverse */
   0,                         /* tp_clear */
   0,                         /* tp_richcompare */
   0,                         /* tp_weaklistoffset */
   0,                         /* tp_iter */
   0,                         /* tp_iternext */
   Connection_methods,         /* tp_methods */
   Connection_members,         /* tp_members */
   0,                         /* tp_getset */
   0,                         /* tp_base */
   0,                         /* tp_dict */
   0,                         /* tp_descr_get */
   0,                         /* tp_descr_set */
   0,                         /* tp_dictoffset */
   (initproc)Connection_init,  /* tp_init */
   0,                         /* tp_alloc */
   0,                         /* tp_new */
};

/*
import spam
connection = spam._connection("hdnprd-c01-r03-01:7222,hdnprd-c01-r04-01:7222,hdnprd-c01-r05-01:7222")
connection.open()

table = spam._table(connection, '/app/SubscriptionBillingPlatform/testInteractive')
table.row('row-000')
connection.close()
table.row('row-000')
*/

typedef struct {
    PyObject_HEAD
    Connection *connection;
    // Do I need to INCREF/DECREF this since I am exposing it to the python layer?
    // Is it better or worse taht this is char * instead of PyObject * ?
    char *table_name;
    //pthread_mutex_t mutex;
    //uint64_t count;
    //PyObject *ret;
    //PyObject *rets;
} Table;

/*
The HBase C API uses callbacks for everything.
The callbacks will increment the table->count, which is used to track if the call back finished
This CallBackBuffer holds a reference to both the table and to the row buf
The call back needs to free the row buf and increment the count when its done
*/

struct BatchCallBackBuffer;

struct CallBackBuffer {
    RowBuffer *rowBuf;
    Table *table;
    int err;
    PyObject *ret;
    uint64_t count;
    pthread_mutex_t mutex;
    BatchCallBackBuffer *batch_call_back_buffer;
    //PyObject *rets;
    CallBackBuffer(Table *t, RowBuffer *r, BatchCallBackBuffer *bcbb) {
        table = t;
        rowBuf = r;
        err = 0;
        count = 0;
        batch_call_back_buffer = bcbb;
        mutex = PTHREAD_MUTEX_INITIALIZER;
    }
    ~CallBackBuffer() {
        //printf("CallBackBuffer Destructor\n");
        delete rowBuf;
        //Py_XDECREF(ret);
    }
};

/*
import spam
connection = spam._connection("hdnprd-c01-r03-01:7222,hdnprd-c01-r04-01:7222,hdnprd-c01-r05-01:7222")
connection.open()

table = spam._table(connection, '/app/SubscriptionBillingPlatform/testInteractive')
table.batch([], 10000)
*/
struct BatchCallBackBuffer {
    //CallBackBuffer *call_back_buffers;
    std::vector<CallBackBuffer *> call_back_buffers;
    int number_of_mutations;
    int count;
    int errors;
    pthread_mutex_t mutex;

    BatchCallBackBuffer(int i) {
        number_of_mutations = i;
        //call_back_buffers = (CallBackBuffer *)malloc(number_of_mutations * sizeof(CallBackBuffer *));
        call_back_buffers.reserve(i);
        count = 0;
        errors = 0;
        mutex = PTHREAD_MUTEX_INITIALIZER;
    }
    ~BatchCallBackBuffer() {
        //delete call_back_buffers;
        //printf("BatchCallBackBuffer destructor\n");

        while (call_back_buffers.size() > 0) {
            CallBackBuffer *buf = call_back_buffers.back();
            call_back_buffers.pop_back();
            delete buf; // In row buffer destructor, its delete [] buf ...
            // doesn't work
            //free(buf);
        }
        // doesn't work
        //free(call_back_buffers);
        printf("After BatchCallBack desructors\n");

    }

};

static CallBackBuffer *CallBackBuffer_create(Table *table, RowBuffer *rowBuf) {
    CallBackBuffer *call_back_buffer = (CallBackBuffer *) malloc(sizeof(CallBackBuffer));
    call_back_buffer->table = table;
    call_back_buffer->rowBuf = rowBuf;
    return call_back_buffer;
}

static void Table_dealloc(Table *self) {
    Py_XDECREF(self->connection);
}

/*
import spam
connection = spam._connection("hdnprd-c01-r03-01:7222,hdnprd-c01-r04-01:7222,hdnprd-c01-r05-01:7222")
connection.open()

table = spam._table(connection, '/app/SubscriptionBillingPlatform/testInteracasdfasdtive')
*/
static int Table_init(Table *self, PyObject *args, PyObject *kwargs) {
    Connection *connection, *tmp;
    char *table_name = NULL;

    if (!PyArg_ParseTuple(args, "O!s", &ConnectionType ,&connection, &table_name)) {
        return -1;
    }

    if (!connection->is_open) {
        Connection_open(connection);
    }

    //int err = hb_admin_table_exists(self->connection->admin, NULL, self->table_name);
    int err = hb_admin_table_exists(connection->admin, NULL, table_name);
    CHECK_RC_RETURN(err);

    if (err != 0) {
        // I guess I have to return -1 nothing else to cause the correct failure
        PyErr_SetString(PyExc_ValueError, "Table does not exist\n");
        //return NULL; // return err;
        return -1;
    }

    // Oddly, if I set self->connection before the above error chech/raise exception
    // If I make a table() that fails because thet able doesn't exist
    // The next time I touch connection I get a seg fault?
    self->table_name = table_name;
    tmp = self->connection;
    Py_INCREF(connection);
    self->connection = connection;
    Py_XDECREF(connection);

    //self->mutex = PTHREAD_MUTEX_INITIALIZER;
    //self->count = 0;

    return 0;
}

// TODO Should I prevent the user from changing the name of the table as it will have no affect?
// Or should changing the name actually change the table?

static PyMemberDef Table_members[] = {
    {"table_name", T_STRING, offsetof(Table, table_name), 0, "The name of the MapRDB table"},
    {NULL}
};


static int read_result(hb_result_t result, PyObject *dict) {
    if (!result) {
        //Py_RETURN_NONE;
        return 1;
    }
    if (!dict) {
        return 1;
    }

    size_t cellCount = 0;
    // Do I need to error check this?
    hb_result_get_cell_count(result, &cellCount);

    // Probably take this as a parameter
    //PyObject *dict = PyDict_New();

    int err = 0;

    for (size_t i = 0; i < cellCount; ++i) {
        const hb_cell_t *cell;
         // Do I need to error check this?
        hb_result_get_cell_at(result, i, &cell);
        if (!cell) {
            printf("cell was null\n");
            return 12;
        }

        int value_len = cell->value_len;
        char *value_cell = (char *) cell->value;
        char *value_char = (char *) malloc(1 + value_len);
        strncpy(value_char, value_cell, value_len);
        printf("cell->value is %s value_char is %s value_len is %i strlen(value_char) is %i\n", value_cell, value_char, value_len, strlen(value_char));
        value_char[value_len] = '\0';
        printf("cell->value is %s value_char is %s value_len is %i strlen(value_char) is %i\n", value_cell, value_char, value_len, strlen(value_char));

        // Set item steals the ref right? No need to INC/DEC?
        // No it doesn't https://docs.python.org/2/c-api/dict.html?highlight=pydict_setitem#c.PyDict_SetItem
        //Py_BuildValue() may run out of memory, and this should be checked
        // Hm I'm not sure if I have to decref Py_BuildValue for %s, Maybe its only %O
        // http://stackoverflow.com/questions/5508904/c-extension-in-python-return-py-buildvalue-memory-leak-problem
        // TODO Does Py_BuildValue copy in the contents or take the pointer? hbase_fqcolumn is mallocing a pointer and returning the pointer...
        // For now I'll free it a few lines down
        char *fq = hbase_fqcolumn(cell);

        if (!fq) {
            printf("fq was null\n");
            return 12;//ENOMEM Cannot allocate memory
        }
        PyObject *key = Py_BuildValue("s", fq);
        free(fq);
        printf("after free fq\n");
        //PyObject *value = Py_BuildValue("s",(char *)cell->value);
        PyObject *value = Py_BuildValue("s", value_char);
        if (!key || !value) {
            printf("key or value was null\n");
            return 12; //ENOMEM Cannot allocate memory
        }
        free(value_char);
        printf("keys ref count is %i\n", key->ob_refcnt);
        //PyDict_SetItem(dict, Py_BuildValue("s", hbase_fqcolumn((char *)cell->family, (char *)cell->qualifier)), Py_BuildValue("s",(char *)cell->value));
        err = PyDict_SetItem(dict, key, value);
        if (err != 0) {
            printf("PyDict_SetItem failed\n");
            // Is this check necessary?
            return err;
        }
        printf("keys ref count after set item is %i\n", key->ob_refcnt);
        // TODO Do I need to decref key and value?

    }

    return 0;
}

/*
Make absolutely certain that the count is set to 1 in all possible exit scenarios
Or else the calling function will hang.
*/
static void row_callback(int32_t err, hb_client_t client, hb_get_t get, hb_result_t result, void *extra) {
    // What should I do if this is null?
    // There is no way to set the count and it will just hang.
    // I suppose its better to crash the program?
    // Maybe if there was some global count I could increment and check for?
    CallBackBuffer *call_back_buffer = (CallBackBuffer *) extra;

    if (err != 0) {
        printf("MapR API failed in row callback %i\n", err);
        pthread_mutex_lock(&call_back_buffer->mutex);
        call_back_buffer->err = err;
        call_back_buffer->count = 1;
        pthread_mutex_unlock(&call_back_buffer->mutex);
        return;
    }
    //call_back_buffer->table->count = 1;

    if (!result) {
        printf("result is null\n");
        // Note that if there is no row for the rowkey, result is not NULL
        // I doubt err wouldn't be 0 if result is null
        pthread_mutex_lock(&call_back_buffer->mutex);
        call_back_buffer->err = 12;
        call_back_buffer->count = 1;
        pthread_mutex_unlock(&call_back_buffer->mutex);
        return;
    }

    //const byte_t *key;
    //size_t keyLen;
    // This returns the rowkey even if there is no row for this rowkey
    //hb_result_get_key(result, &key, &keyLen);
    //printf("key is %s\n", key);

    // Do I need to dec ref? I don't know, memory isn't increasing when i run this in a loop
    PyObject *dict = PyDict_New();
    if (!dict) {
        printf("dict is null\n");
        pthread_mutex_lock(&call_back_buffer->mutex);
        call_back_buffer->err = 12;
        call_back_buffer->count = 1;
        pthread_mutex_unlock(&call_back_buffer->mutex);
        return;
    }

    err = read_result(result, dict);
    if (err != 0) {
        printf("read result was %i", call_back_buffer->err);
        pthread_mutex_lock(&call_back_buffer->mutex);
        call_back_buffer->err = err;
        call_back_buffer->count = 1;
        pthread_mutex_unlock(&call_back_buffer->mutex);
        return;
    }
    //call_back_buffer->table->ret = dict;
    pthread_mutex_lock(&call_back_buffer->mutex);
    call_back_buffer->ret = dict;
    call_back_buffer->count = 1;
    pthread_mutex_unlock(&call_back_buffer->mutex);

    hb_result_destroy(result);
    hb_get_destroy(get);
}
/*
import spam
connection = spam._connection("hdnprd-c01-r03-01:7222,hdnprd-c01-r04-01:7222,hdnprd-c01-r05-01:7222")
connection.open()

table = spam._table(connection, '/app/SubscriptionBillingPlatform/testInteractive')
table.row('hello')
*/

/*
This has a memory leak:
top -b | grep python

import spam
connection = spam._connection("hdnprd-c01-r03-01:7222,hdnprd-c01-r04-01:7222,hdnprd-c01-r05-01:7222")
connection.open()

table = spam._table(connection, '/app/SubscriptionBillingPlatform/testInteractive')
print table.table_name
table.row('hello')
print table.table_name

# TODO LOL REALLY WEIRD BUG IF I LEAVE THE COMMENT IN TABLE NAME GETS CHANGED???

while True:
    # Leaks for both no result and for result
    table.row('hello')
*/

static PyObject *Table_row(Table *self, PyObject *args) {
    char *row_key;
    if (!PyArg_ParseTuple(args, "s", &row_key)) {
        return NULL;
    }
    if (!self->connection->is_open) {
        Connection_open(self->connection);
    }

    int err = 0;

    hb_get_t get;
    err = hb_get_create((const byte_t *)row_key, strlen(row_key) + 1, &get);
    CHECK_RC_RETURN(err);
    if (err != 0) {
        PyErr_SetString(PyExc_ValueError, "Could not create get");
        return NULL;
    }

    //err = hb_get_set_table(get, tableName, strlen(tableName));
    err = hb_get_set_table(get, self->table_name, strlen(self->table_name));
    CHECK_RC_RETURN(err);
    if (err != 0) {
        PyErr_SetString(PyExc_ValueError, "Could not set table name on get");
        return NULL;
    }

    // Do I need to check these for null?
    RowBuffer *rowBuf = new RowBuffer();
    CallBackBuffer *call_back_buffer = new CallBackBuffer(self, rowBuf, NULL);

    //self->count = 0;
    //err = hb_get_send(client, get, get_send_cb, rowBuf);
    err = hb_get_send(self->connection->client, get, row_callback, call_back_buffer);
    CHECK_RC_RETURN(err);
    if (err != 0) {
        PyErr_SetString(PyExc_ValueError, "Could not send get");
        return NULL;
    }

    int wait = 0;
    //while (self->count != 1) {
    while (call_back_buffer->count != 1) {
        sleep(0.1);
        wait += 1;
        if (wait == 20) {
            //PyErr_SetString(SpamError, "Library error");
            //return NULL;
        }
    }

    //return self->ret;
    PyObject *ret = call_back_buffer->ret;
    err = call_back_buffer->err;
    delete call_back_buffer;
    if (err == 0) {
        return ret;
    }
    PyErr_SetString(PyExc_ValueError, "Error in get callback");
    return NULL;
}


void client_flush_callback(int32_t err, hb_client_t client, void *ctx) {
    //printf("Client flush callback invoked: %d\n", err);
    CHECK_RC_RETURN(err);
    if (err != 0) {

    }
}

/*

static int split(char *fq, char* arr[]) {
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

    //printf("arr[0] is %s\n", arr[0]);
    //printf("arr[1] is %s\n", arr[1]);

}
*/
/*
import spam
connection = spam._connection("hdnprd-c01-r03-01:7222,hdnprd-c01-r04-01:7222,hdnprd-c01-r05-01:7222")
connection.open()

table = spam._table(connection, '/app/SubscriptionBillingPlatform/testInteractive')
table.put("snoop", {"f:foo": "bar"})
*/
static int split(char *fq, char *family, char *qualifier) {
    int i = 0;
    // Initialize family to length + null pointer - 1 for the colon
    bool found_colon = false;
    for (i = 0; i < strlen(fq) && fq[i] != '\0'; i++) {
        if (fq[i] != ':') {
            family[i] = fq[i];
        } else {
            found_colon = true;
            break;
        }
    }
    family[i] = '\0';

    if (!found_colon) {
        return -10;
    }

    // This works with 1+ or without 1+ ...
    int qualifier_index = 0;
    for (i=i + 1; i < strlen(fq) && fq[i] != '\0'; i++) {
        qualifier[qualifier_index] = fq[i];
        qualifier_index += 1;
    }
    qualifier[qualifier_index] = '\0';

    //arr[0] = family;
    //arr[1] = qualifier;

    //printf("arr[0] is %s\n", arr[0]);
    //printf("arr[1] is %s\n", arr[1]);
    return 0;

}





void put_callback(int err, hb_client_t client, hb_mutation_t mutation, hb_result_t result, void *extra) {
    // TODO hb_mutation_set_bufferable
    /*

     * Sets whether or not this RPC can be buffered on the client side.
     *
     * Currently only puts and deletes can be buffered. Calling this for
     * any other mutation type will return EINVAL.
     *
     * The default is true.

    HBASE_API int32_t
    hb_mutation_set_bufferable(
        hb_mutation_t mutation,
        const bool bufferable);
     */

    // TODO Check types.h for the HBase error codes
    CallBackBuffer *call_back_buffer = (CallBackBuffer *) extra;

    if (err != 0) {
        printf("MapR API Failed on Put Callback %i\n", err);
        pthread_mutex_lock(&call_back_buffer->mutex);
        call_back_buffer->count = 1;
        call_back_buffer->err = err;
        pthread_mutex_unlock(&call_back_buffer->mutex);
        if (call_back_buffer->batch_call_back_buffer) {
            pthread_mutex_lock(&call_back_buffer->batch_call_back_buffer->mutex);
            call_back_buffer->batch_call_back_buffer->errors++;
            call_back_buffer->batch_call_back_buffer->count++;
            pthread_mutex_unlock(&call_back_buffer->batch_call_back_buffer->mutex);
        }
        return;
    }

    /*
    // It looks like result is always NULL for put?
    if (!result) {
        printf("result is null!\n");
        call_back_buffer->err = 12; // OOM
        call_back_buffer->count = 1;
        if (call_back_buffer->batch_call_back_buffer) {
            pthread_mutex_lock(&call_back_buffer->table->mutex);
            call_back_buffer->batch_call_back_buffer->count++;
            call_back_buffer->batch_call_back_buffer->errors++;
            pthread_mutex_unlock(&call_back_buffer->table->mutex);
        }
    }
    */

    pthread_mutex_lock(&call_back_buffer->mutex);
    call_back_buffer->count = 1;
    pthread_mutex_unlock(&call_back_buffer->mutex);
    if (call_back_buffer->batch_call_back_buffer) {
        pthread_mutex_lock(&call_back_buffer->batch_call_back_buffer->mutex);
        call_back_buffer->batch_call_back_buffer->count++;
        pthread_mutex_unlock(&call_back_buffer->batch_call_back_buffer->mutex);
    }
    hb_mutation_destroy(mutation);
    //hb_result_destroy(result);


    //hb_mutation_destroy(mutation);
    //pthread_mutex_lock(&call_back_buffer->table->mutex);
    //table->count++;
    //call_back_buffer->count++;
    //pthread_mutex_unlock(&call_back_buffer->table->mutex);
    //printf("after lock thing\n");
    //printf("we have a result\n");

    //hb_result_destroy(result);

    // Ya this is important to do for puts lol
    //if (extra) {
        //RowBuffer *rowBuf = (RowBuffer *)extra;
        //CallBackBuffer *call_back_buffer = (CallBackBuffer *) extra;
        //printf("before delete buffer\n");
        //delete call_back_buffer; //->rowBuf;
        //printf("after delete buffer\n");
        //free(call_back_buffer);
    //}

    /*

    hb_mutation_destroy(mutation);
    pthread_mutex_lock(&call_back_buffer->table->mutex);
    //table->count++;
    call_back_buffer->table->count++;
    pthread_mutex_unlock(&call_back_buffer->table->mutex);
    //printf("after lock thing\n");
    //printf("we have a result\n");
    const byte_t *key;
    size_t keyLen;
    //printf("before hb_result_get_key\n");
    hb_result_get_key(result, &key, &keyLen);
    //printf("after hb_result_get_key\n");
    //printf("Row: %s\t", (char *)key);
    read_result(result, NULL);
    //printf("\n");
    hb_result_destroy(result);

    // Ya this is important to do for puts lol
    //if (extra) {
        //RowBuffer *rowBuf = (RowBuffer *)extra;
        //CallBackBuffer *call_back_buffer = (CallBackBuffer *) extra;
        //printf("before delete buffer\n");
        delete call_back_buffer; //->rowBuf;
        //printf("after delete buffer\n");
        //free(call_back_buffer);
    //}
    */

}

void create_dummy_cell(hb_cell_t **cell,
                      const char *r, size_t rLen,
                      const char *f, size_t fLen,
                      const char *q, size_t qLen,
                      const char *v, size_t vLen) {
    // Do I need to check this
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


/*
import spam
connection = spam._connection("hdnprd-c01-r03-01:7222,hdnprd-c01-r04-01:7222,hdnprd-c01-r05-01:7222")
connection.open()

table = spam._table(connection, '/app/SubscriptionBillingPlatform/testInteractive')
table.put('snoop', {'Name:a':'a','Name:foo':'bar'})
for i in range(1000000):
    table.put('snoop', {'Name:a':'a','Name:foo':'bar'})

lol()
*/



static int make_put(Table *self, RowBuffer *rowBuf, const char *row_key, PyObject *dict, hb_put_t *hb_put) {
    int err;
    //hb_put_t *hb_put = (hb_put_t *) malloc(sizeof(hb_put_t));
    //printf("Before hb_put_create\n");

    int size = PyDict_Size(dict);
    if (size < 1) {
        // TODO for Batch puts do we not want to set the exception message here, but rather in the batch()?
        PyErr_SetString(PyExc_ValueError, "Row contents are empty");
        return -5;
    }

    err = hb_put_create((byte_t *)row_key, strlen(row_key), hb_put);
    CHECK_RC_RETURN(err);
    if (err != 0) {
        PyErr_SetString(PyExc_ValueError, "Could not create put");
        return err;
    }

    PyObject *fq, *value;
    Py_ssize_t pos = 0;
    hb_cell_t *cell;
    //char *arr[2];
    //char arr[2][1024];
    // https://docs.python.org/2/c-api/dict.html?highlight=pydict_next#c.PyDict_Next
    // This says PyDict_Next borrows references for key and value...
    while (PyDict_Next(dict, &pos, &fq, &value)) {
        // Its weird if I loop batch with 100000, the ref count is 100002 for value??
        //printf("value ref count is %i\n", value->ob_refcnt);
        char *fq_char = PyString_AsString(fq);
        if (!fq_char or strlen(fq_char) == 0) {
            printf("Null or empty fq\n");
            return -1;
        }
        char *family = rowBuf->getBuffer(strlen(fq_char)); // Don't +1 for null terminator, because of colon
        char *qualifier = rowBuf->getBuffer(strlen(fq_char)); // Don't +1 for null terminator, because of colon
        err = split(fq_char, family, qualifier);
        if (err != 0) {
            return err;
        }
        //printf("family is %s\n", arr[0]);
        //printf("qualifier is %s\n", arr[1]);
        // TODO Have to make sure to free this memory lol
        //char *family = rowBuf->getBuffer(1024);
        //char *qualifier = rowBuf->getBuffer(1024);
        char *value_char = PyString_AsString(value);
        if (!value_char) {
            printf("value_char is null in make_put\n");
            return -1;
        }
        //char *v = rowBuf->getBuffer(strlen(value_char) + 1);
        char *v = rowBuf->getBuffer(strlen(value_char));


        //strcpy(family, arr[0]);
        //strcpy(qualifier, arr[1]);
        // delete [] arr;
        strcpy(v, value_char);
        //free(arr); //this throws an error lol

        //printf("family is %s\n", family);
        //printf("qualifier is %s\n", qualifier);
        //printf("v is %s\n", v);

        //printf("creating dummy cell\n");
        // How come I wasn't doing +1 on row key??
        //create_dummy_cell(&cell, row_key, strlen(row_key), family, strlen(family) + 1, qualifier, strlen(qualifier) + 1, v, strlen(v) + 1);
        printf("strlen(family) is %i\n", strlen(family));
        printf("strlen(qualifier) is %i\n", strlen(qualifier));
        create_dummy_cell(&cell, row_key, strlen(row_key), family, strlen(family), qualifier, strlen(qualifier), v, strlen(v));
        //printf("put add cell\n");
        err = hb_put_add_cell(*hb_put, cell);;
        CHECK_RC_RETURN(err);
        if (err != 0) {
            PyErr_SetString(PyExc_ValueError, "Could not add cell to put");
            return -1;
        }
        //printf("put add cell error %i\n", err);
        delete cell;
        //printf("RC for put add cell was %i\n", err);

    }

    //printf("hb_mutation set table\n");
    err = hb_mutation_set_table((hb_mutation_t)*hb_put, self->table_name, strlen(self->table_name));
    CHECK_RC_RETURN(err);
    if (err != 0) {
        PyErr_SetString(PyExc_ValueError, "Could not add cell to put");
        return err;
    }
    //printf("RC for muttaion set table was %i\n", err);

    return err;
}

static PyObject *Table_put(Table *self, PyObject *args) {
    char *row_key;
    PyObject *dict;

    if (!PyArg_ParseTuple(args, "sO!", &row_key, &PyDict_Type, &dict)) {
        return NULL;
    }

    int err = 0;

    RowBuffer *rowBuf = new RowBuffer();
    CallBackBuffer *call_back_buffer = new CallBackBuffer(self, rowBuf, NULL);

    hb_put_t hb_put;
    err = make_put(self, rowBuf, row_key, dict, &hb_put);
    CHECK_RC_RETURN(err);
    if (err != 0) {
        // This would just override the error message set in make_put
        // PyErr_SetString(PyExc_ValueError, "Could not create put oh noo");
        if (err == -10) {
            PyErr_SetString(PyExc_ValueError, "All keys must contain a colon delimiting the family and qualifier");
        }
        return NULL;
    }

    err = hb_mutation_send(self->connection->client, (hb_mutation_t)hb_put, put_callback, call_back_buffer);
    CHECK_RC_RETURN(err);
    if (err != 0) {
        PyErr_SetString(PyExc_ValueError, "Could not send put");
        return NULL;
    }


    //self->count = 0;
    hb_client_flush(self->connection->client, client_flush_callback, NULL);
    //printf("Waiting for all callbacks to return ...\n");

    //uint64_t locCount;
    //do {
    //    sleep (1);
    //    pthread_mutex_lock(&mutex);
    //    locCount = count;
    //    pthread_mutex_unlock(&mutex);
    //} while (locCount < numRows);

    int wait = 0;

    //while (self->count != 1) {
    // TODO Do I need to aquire the lock on this?
    while (call_back_buffer->count != 1) {
        sleep(0.1);
        wait++;
        //printf("wait is %i\n",wait);
        if (wait == 20) {
            //printf("wait is %i\n",wait);
            //PyErr_SetString(SpamError, "Library error");
            //return NULL;
        }

    }

    //free(hb_put);
    err = call_back_buffer->err;
    delete call_back_buffer;
    CHECK_RC_RETURN(err);
    if (err != 0) {
        if (err == 2) {
            PyErr_SetString(PyExc_ValueError, "Error in put callback, probably bad column family");
        } else {
            PyErr_SetString(PyExc_ValueError, "Unknown Error in put callback");
        }

        return NULL;
    }

    Py_RETURN_NONE;
}

void scan_callback(int32_t err, hb_scanner_t scan, hb_result_t *results, size_t numResults, void *extra) {
    //printf("In sn_cb\n");
    printf("In scan callback\n");

    CallBackBuffer *call_back_buffer = (CallBackBuffer *) extra;
    if (err != 0) {
        pthread_mutex_lock(&call_back_buffer->mutex);
        call_back_buffer->err = err;
        call_back_buffer->count = 1;
        pthread_mutex_unlock(&call_back_buffer->mutex);
        return;
    }
    if (!results) {
        pthread_mutex_lock(&call_back_buffer->mutex);
        call_back_buffer->err = 12;
        call_back_buffer->count = 1;
        pthread_mutex_unlock(&call_back_buffer->mutex);
        return;
    }

    if (numResults > 0) {
        //printf("bnefore rowbuff = extra\n");

        PyObject *dict;
        //printf("befoer loop\n");
        for (uint32_t r = 0; r < numResults; ++r) {
            //printf("looping\n");
            const byte_t *key;
            size_t keyLen;
            // API doesn't document when this returns something other than 0
            err = hb_result_get_key(results[r], &key, &keyLen);
            if (err != 0) {
                pthread_mutex_lock(&call_back_buffer->mutex);
                call_back_buffer->err = err;
                call_back_buffer->count = 1;
                pthread_mutex_unlock(&call_back_buffer->mutex);
                return;
            }

            char *key_char = (char *) malloc(1 + keyLen);
            strncpy(key_char, (char *)key, keyLen);
            key_char[keyLen] = '\0';

            printf("key is %s keyLen is %i key_char is %s\n", key, keyLen, key_char);

            //printf("Row: %s\t", (char *)key);
            // Do I need a null check?
            dict = PyDict_New();
            if (!dict) {
                pthread_mutex_lock(&call_back_buffer->mutex);
                call_back_buffer->err = 12;
                call_back_buffer->count = 1;
                pthread_mutex_unlock(&call_back_buffer->mutex);
                return;
            }
            //printf("dicts ref count %i\n", dict->ob_refcnt);
            //printf("before reading result into dict\n");
            err = read_result(results[r], dict);
            if (err != 0) {
                pthread_mutex_lock(&call_back_buffer->mutex);
                call_back_buffer->err = err;
                call_back_buffer->count = 1;
                pthread_mutex_unlock(&call_back_buffer->mutex);
                Py_DECREF(dict);
                // TODO If I decref this will i seg fault if i access it later?
                // Should itb e set to a none?
                return;
            }
            //printf("dicts ref count after read_results %i\n", dict->ob_refcnt);
            //printf("before append\n");
            // Do I need to INCREF the result of Py_BuildValue?
            // Should I do that ! with the type? Does that make it faster or slower lol
            //PyObject *tuple = Py_BuildValue("sO",(char *)key, dict);
            PyObject *tuple = Py_BuildValue("sO",(char *)key_char, dict);
            free(key_char);
            if (!tuple) {
                pthread_mutex_lock(&call_back_buffer->mutex);
                call_back_buffer->err = 12;
                call_back_buffer->count = 1;
                pthread_mutex_unlock(&call_back_buffer->mutex);
                Py_DECREF(dict);
                return;
            }

            err = PyList_Append(call_back_buffer->ret, tuple);
            if (err != 0) {
                pthread_mutex_lock(&call_back_buffer->mutex);
                call_back_buffer->err = err;
                call_back_buffer->count = 1;
                pthread_mutex_unlock(&call_back_buffer->mutex);
                Py_DECREF(dict);
                Py_DECREF(tuple);
                // TODO If I decref this will i seg fault if i access it later?
                // Should itb e set to a none?
                return;
            }
            //printf("dicts ref count after append %i\n", dict->ob_refcnt);

            Py_DECREF(dict);
            //printf("dicts ref count after decref %i", dict->ob_refcnt);

            //printf("\n");
            //printf("before destroy\n");
            hb_result_destroy(results[r]);
        }
        // The API doesn't specify when the return value would not be 0
        // But it is used in this unittest:
        // https://github.com/mapr/libhbase/blob/0ddda015113452955ed600116f58a47eebe3b24a/src/test/native/unittests/libhbaseutil.cc#L760
        err = hb_scanner_next(scan, scan_callback, call_back_buffer);
        CHECK_RC_RETURN(err);
        if (err != 0) {
            //PyErr_SetString(PyExc_ValueError, "Failed in scanner callback");
            pthread_mutex_lock(&call_back_buffer->mutex);
            call_back_buffer->err = err;
            call_back_buffer->count = 1;
            pthread_mutex_unlock(&call_back_buffer->mutex);
            return;
        }
    } else {
        //sleep(0.1);
        // TODO Is it necessary to aquire the lock here? Isn't there only going to be one thread on this?
        pthread_mutex_lock(&call_back_buffer->mutex);
        call_back_buffer->count = 1;
        pthread_mutex_unlock(&call_back_buffer->mutex);
    }
}

/*
import spam
connection = spam._connection("hdnprd-c01-r03-01:7222,hdnprd-c01-r04-01:7222,hdnprd-c01-r05-01:7222")
connection.open()

table = spam._table(connection, '/app/SubscriptionBillingPlatform/testInteractive')
table.scan('hello', 'hello100~')
*/

static PyObject *Table_scan(Table *self, PyObject *args) {
    char *start = "";
    char *stop = "";

    if (!PyArg_ParseTuple(args, "|ss", &start, &stop)) {
        return NULL;
    }

    int err = 0;

    hb_scanner_t scan;
    err = hb_scanner_create(self->connection->client, &scan);
    CHECK_RC_RETURN(err);
    if (err != 0) {
        PyErr_SetString(PyExc_ValueError, "Failed to create the scanner");
        return NULL;
    }

    err = hb_scanner_set_table(scan, self->table_name, strlen(self->table_name));
    CHECK_RC_RETURN(err);
    if (err != 0) {
        PyErr_SetString(PyExc_ValueError, "Failed to set table on scanner");
        return NULL;
    }

    // TODO parameratize this
    err = hb_scanner_set_num_versions(scan, 1);
    CHECK_RC_RETURN(err);
    if (err != 0) {
        PyErr_SetString(PyExc_ValueError, "Failed to set num versions on scanner");
        return NULL;
    }

    if (strlen(start) > 0) {
        // Do I need strlen + 1 ?
        err = hb_scanner_set_start_row(scan, (byte_t *) start, strlen(start));
        CHECK_RC_RETURN(err);
        if (err != 0) {
            PyErr_SetString(PyExc_ValueError, "Failed to set start row on scanner");
            return NULL;
        }
    }
    if (strlen(stop) > 1) {
        // do I need strlen + 1 ?
        err = hb_scanner_set_end_row(scan, (byte_t *) stop, strlen(stop));
        CHECK_RC_RETURN(err);
        if (err != 0) {
            PyErr_SetString(PyExc_ValueError, "Failed to set stop row on scanner");
            return NULL;
        }
    }

    // Does it optimize if I set this higher?
    // TODO what is this?
    err = hb_scanner_set_num_max_rows(scan, 1);
    CHECK_RC_RETURN(err);
    if (err != 0) {
        PyErr_SetString(PyExc_ValueError, "Failed to set num_max_rows scanner");
        return NULL;
    }

    RowBuffer *rowBuf = new RowBuffer();
    CallBackBuffer *call_back_buffer = new CallBackBuffer(self, rowBuf, NULL);
    //self->rets = PyList_New(0);
    call_back_buffer->ret = PyList_New(0);


    err = hb_scanner_next(scan, scan_callback, call_back_buffer);
    CHECK_RC_RETURN(err);
    if (err != 0) {
        PyErr_SetString(PyExc_ValueError, "Failed in scanner callback");
        return NULL;
    }

    int wait = 0;
    while (call_back_buffer->count != 1) {
        sleep(0.1);
        wait += 1;
    }

    // TODO I need to free this right
    PyObject *ret = call_back_buffer->ret;
    printf("ret has ref count of %i\n", ret->ob_refcnt);
    err = call_back_buffer->err;
    delete call_back_buffer;
    printf("after delete call back buffer has ref count of %i\n", ret->ob_refcnt);
    if (err == 0) {
        return ret;
    }
    PyErr_SetString(PyExc_ValueError, "Error in scanner callback");
    return NULL;
}

void delete_callback(int err, hb_client_t client, hb_mutation_t mutation, hb_result_t result, void *extra) {
    // Not sure if I can just use the put_callback or if I should change them

    //Table *table = (Table *) extra;
    CallBackBuffer *call_back_buffer = (CallBackBuffer *) extra;

    if (err != 0) {
        printf("MapR API Failed on Delete Callback %i\n", err);
        // TODO Do I need to aquire the lock
        pthread_mutex_lock(&call_back_buffer->mutex);
        call_back_buffer->err = err;
        call_back_buffer->count = 1;
        pthread_mutex_unlock(&call_back_buffer->mutex);
        if (call_back_buffer->batch_call_back_buffer) {
            pthread_mutex_lock(&call_back_buffer->batch_call_back_buffer->mutex);
            call_back_buffer->batch_call_back_buffer->errors++;
            call_back_buffer->batch_call_back_buffer->count++;
            pthread_mutex_unlock(&call_back_buffer->batch_call_back_buffer->mutex);
        }

        return;
    }

    /*
    // It looks like result is always NULL for delete?
    if (!result) {
        printf("result is null!\n");
        call_back_buffer->err = 12; // OOM
        call_back_buffer->count = 1;
        if (call_back_buffer->batch_call_back_buffer) {
            pthread_mutex_lock(&call_back_buffer->table->mutex);
            call_back_buffer->batch_call_back_buffer->count++;
            call_back_buffer->batch_call_back_buffer->errors++;
            pthread_mutex_unlock(&call_back_buffer->table->mutex);
        }
    }
    */


    // TODO Do I need to aquire a lock here
    pthread_mutex_lock(&call_back_buffer->mutex);
    call_back_buffer->count = 1;
    pthread_mutex_unlock(&call_back_buffer->mutex);
    if (call_back_buffer->batch_call_back_buffer) {
        pthread_mutex_lock(&call_back_buffer->batch_call_back_buffer->mutex);
        call_back_buffer->batch_call_back_buffer->count++;
        pthread_mutex_unlock(&call_back_buffer->batch_call_back_buffer->mutex);
    }

}

/*
import spam
connection = spam._connection("hdnprd-c01-r03-01:7222,hdnprd-c01-r04-01:7222,hdnprd-c01-r05-01:7222")
connection.open()

table = spam._table(connection, '/app/SubscriptionBillingPlatform/testInteractive')
table.row('hello1')
table.delete('hello1')
*/

static int make_delete(Table *self, char *row_key, hb_delete_t *hb_delete) {
    int err = 0;

    if (strlen(row_key) == 0) {
        err = -5;
        PyErr_SetString(PyExc_ValueError, "row_key was empty string");
        return err;
    }

    //err = hb_delete_create((byte_t *)row_key, strlen(row_key) + 1, hb_delete);
    err = hb_delete_create((byte_t *)row_key, strlen(row_key), hb_delete);
    CHECK_RC_RETURN(err);
    if (err != 0) {
        PyErr_SetString(PyExc_ValueError, "Could not create delete");
        return err;
    }

    err = hb_mutation_set_table((hb_mutation_t)*hb_delete, self->table_name, strlen(self->table_name));
    CHECK_RC_RETURN(err);
    if (err != 0) {
        PyErr_SetString(PyExc_ValueError, "Could not set table on delete");
        return err;
    }

    return err;
}

static PyObject *Table_delete(Table *self, PyObject *args) {
    char *row_key;
    if (!PyArg_ParseTuple(args, "s", &row_key)) {
        return NULL;
    }
    if (!self->connection->is_open) {
        Connection_open(self->connection);
    }

    int err = 0;

    hb_delete_t hb_delete;
    err = make_delete(self, row_key, &hb_delete);
    CHECK_RC_RETURN(err);
    if (err != 0) {
        // Try this and see what happens
        //PyErr_SetString(PyExc_ValueError, "Could not create delete");
        return NULL;
    }
    printf("RC for muttaion set table was %i\n", err);

    RowBuffer *rowBuf = new RowBuffer();
    //CallBackBuffer *call_back_buffer = CallBackBuffer_create(self, rowBuf);
    CallBackBuffer *call_back_buffer = new CallBackBuffer(self, rowBuf, NULL);

    err = hb_mutation_send(self->connection->client, (hb_mutation_t)hb_delete, delete_callback, call_back_buffer);
    CHECK_RC_RETURN(err);
    if (err != 0) {
        PyErr_SetString(PyExc_ValueError, "Could not send delete");
        return NULL;
    }
    printf("RC for mutation send was %i\n", err);

    // Todo do I need to error check this?
    hb_client_flush(self->connection->client, client_flush_callback, NULL);
    printf("Waiting for all callbacks to return ...\n");

    int wait = 0;

    while (call_back_buffer->count != 1) {
        sleep(0.1);
        wait++;
        //printf("wait is %i\n",wait);
        if (wait == 20) {
            //printf("wait is %i\n",wait);
            //PyErr_SetString(SpamError, "Library error");
            //return NULL;
        }

    }

    err = call_back_buffer->err;
    delete call_back_buffer;
    CHECK_RC_RETURN(err);
    if (err != 0) {
        PyErr_SetString(PyExc_ValueError, "Error in delete callback");
        return NULL;
    }

    Py_RETURN_NONE;
}


/*
import spam
connection = spam._connection("hdnprd-c01-r03-01:7222,hdnprd-c01-r04-01:7222,hdnprd-c01-r05-01:7222")
connection.open()

table = spam._table(connection, '/app/SubscriptionBillingPlatform/testInteractive')
table.batch([('put', 'hello{}'.format(i), {'Name:bar':'bar{}'.format(i)}) for i in range(100000)])
#table.scan()


import spam
connection = spam._connection("hdnprd-c01-r03-01:7222,hdnprd-c01-r04-01:7222,hdnprd-c01-r05-01:7222")
connection.open()

table = spam._table(connection, '/app/SubscriptionBillingPlatform/testInteractive')
table.batch([('delete', 'hello{}'.format(i), {'Name:bar':'bar{}'.format(i)}) for i in range(100000)])



table.batch([], 10000)
table.batch([None for _ in range(1000000)], 10)
table.batch([('delete', 'hello{}'.format(i)) for i in range(100000)])

*/

/*
static PyObject *Table_batch(Table *self, PyObject *args) {

    PyObject *actions;
    //int number_of_mutations;

    if (!PyArg_ParseTuple(args, "O!", &PyList_Type, &actions)) {
        return NULL;
    }

    //int number_of_mutations = 1000;
    int number_of_actions = PyList_Size(actions);
    BatchCallBackBuffer *batch_cbb = new BatchCallBackBuffer(number_of_actions);
    int i;
    PyObject *value;
    for (i = 0; i < number_of_actions; i++) {
        value = PyList_GetItem(actions, i);
        RowBuffer *rowBuf = new RowBuffer();
        char *rk = rowBuf->getBuffer(1024);
        strcpy(rk, PyString_AsString(value));
        CallBackBuffer *call_back_buffer = new CallBackBuffer(self, rowBuf, batch_cbb);
        call_back_buffer->count = i;
        batch_cbb->call_back_buffers.push_back(call_back_buffer);
    }

    delete batch_cbb;

    Py_RETURN_NONE;
}
*/

static PyObject *Table_batch(Table *self, PyObject *args) {
    PyObject *actions;

    if (!PyArg_ParseTuple(args, "O!", &PyList_Type, &actions)) {
        return NULL;
    }

    int err;

    PyObject *tuple;
    Py_ssize_t i;
    int number_of_actions = PyList_Size(actions);
    BatchCallBackBuffer *batch_call_back_buffer = new BatchCallBackBuffer(number_of_actions);
    for (i = 0; i < number_of_actions; i++) {
        RowBuffer *rowBuf = new RowBuffer();
        CallBackBuffer *call_back_buffer = new CallBackBuffer(self, rowBuf, batch_call_back_buffer);
        batch_call_back_buffer->call_back_buffers.push_back(call_back_buffer);

        tuple = PyList_GetItem(actions, i);
        if (!tuple) {
            // Is this check even necessary
            pthread_mutex_lock(&batch_call_back_buffer->mutex);
            batch_call_back_buffer->errors++;
            batch_call_back_buffer->count++;
            pthread_mutex_unlock(&batch_call_back_buffer->mutex);

            call_back_buffer->count++;
            call_back_buffer->err = 12;
            continue;
        }

        if (!PyTuple_Check(tuple)) {
            pthread_mutex_lock(&batch_call_back_buffer->mutex);
            batch_call_back_buffer->errors++;
            batch_call_back_buffer->count++;
            pthread_mutex_unlock(&batch_call_back_buffer->mutex);

            call_back_buffer->count++;
            call_back_buffer->err = -1; //TODO BETTER
            continue;
        }
        //printf("tuples ref count is %i\n", tuple->ob_refcnt);
        //printf("got tuple\n");
        PyObject *mutation_type = PyTuple_GetItem(tuple, 0);
        if (!mutation_type) {
            // Is this check even necessary
            pthread_mutex_lock(&batch_call_back_buffer->mutex);
            batch_call_back_buffer->errors++;
            batch_call_back_buffer->count++;
            pthread_mutex_unlock(&batch_call_back_buffer->mutex);

            call_back_buffer->count++;
            call_back_buffer->err = 12;
            continue;
        }
        if (!PyString_Check(mutation_type) && !PyUnicode_Check(mutation_type)) {
            pthread_mutex_lock(&batch_call_back_buffer->mutex);
            batch_call_back_buffer->errors++;
            batch_call_back_buffer->count++;
            pthread_mutex_unlock(&batch_call_back_buffer->mutex);

            call_back_buffer->count++;
            call_back_buffer->err = -1; //TODO BETTER
            continue;
        }
        char *mutation_type_char = PyString_AsString(mutation_type);
        if (!mutation_type_char) {
            // Is this check even necessary
            pthread_mutex_lock(&batch_call_back_buffer->mutex);
            batch_call_back_buffer->errors++;
            batch_call_back_buffer->count++;
            pthread_mutex_unlock(&batch_call_back_buffer->mutex);

            call_back_buffer->count++;
            call_back_buffer->err = 12;
            continue;
        }
        //printf("tuples ref count after pystring_asstring %i\n", tuple->ob_refcnt);
        //printf("got mutation_type\n");
        //printf("mutation type is %s\n", mutation_type);
        // TODO CHECK LENGTHS OF PUT VS GET FOR ERRORS



        PyObject *row_key = PyTuple_GetItem(tuple, 1);
        if (!row_key) {
            // Is this check even necessary
            pthread_mutex_lock(&batch_call_back_buffer->mutex);
            batch_call_back_buffer->errors++;
            batch_call_back_buffer->count++;
            pthread_mutex_unlock(&batch_call_back_buffer->mutex);

            call_back_buffer->count++;
            call_back_buffer->err = 12;
            continue;
        }

        if (!PyString_Check(row_key) && !PyUnicode_Check(row_key)) {
            pthread_mutex_lock(&batch_call_back_buffer->mutex);
            batch_call_back_buffer->errors++;
            batch_call_back_buffer->count++;
            pthread_mutex_unlock(&batch_call_back_buffer->mutex);

            call_back_buffer->count++;
            call_back_buffer->err = -1; //TODO BETTER
            continue;
        }

        char *row_key_char = PyString_AsString(row_key);
        if (!row_key_char) {
            // Is this check even necessary
            pthread_mutex_lock(&batch_call_back_buffer->mutex);
            batch_call_back_buffer->errors++;
            batch_call_back_buffer->count++;
            pthread_mutex_unlock(&batch_call_back_buffer->mutex);

            call_back_buffer->count++;
            call_back_buffer->err = 12;
            continue;
        }

        if (strcmp(mutation_type_char, "put") == 0) {
            //printf("Its a put");

            //printf("size of call_back_buffers is %ld\n",sizeof(batch_call_back_buffer->call_back_buffers));
            //In particular, all functions whose function it is to create a new object, such as PyInt_FromLong() and Py_BuildValue(), pass ownership to the receiver.
            //printf("tuples ref count after pystringasstring 1 is %i\n", tuple->ob_refcnt);
            PyObject *dict = PyTuple_GetItem(tuple, 2);
            if (!dict) {
                // Is this check even necessary
                pthread_mutex_lock(&batch_call_back_buffer->mutex);
                batch_call_back_buffer->errors++;
                batch_call_back_buffer->count++;
                pthread_mutex_unlock(&batch_call_back_buffer->mutex);

                call_back_buffer->count++;
                call_back_buffer->err = 12;
                continue;
            }
            if (!PyDict_Check(dict)) {
                pthread_mutex_lock(&batch_call_back_buffer->mutex);
                batch_call_back_buffer->errors++;
                batch_call_back_buffer->count++;
                pthread_mutex_unlock(&batch_call_back_buffer->mutex);

                call_back_buffer->count++;
                call_back_buffer->err = -1;
                continue;
            }
            //printf("tuples ref count after pytuplegetitem 2 %i\n", tuple->ob_refcnt);
            //printf("dict ref count is %i\n", dict->ob_refcnt);
            // do I need to increment dict?
            hb_put_t hb_put;
            err = make_put(self, rowBuf, row_key_char, dict, &hb_put);
            //printf("dict ref count after make put %i\n", dict->ob_refcnt);
            CHECK_RC_RETURN(err)
            if (err != 0) {
                pthread_mutex_lock(&batch_call_back_buffer->mutex);
                batch_call_back_buffer->errors++;
                batch_call_back_buffer->count++;
                pthread_mutex_unlock(&batch_call_back_buffer->mutex);

                call_back_buffer->count++;
                call_back_buffer->err = err;
            }
            err = hb_mutation_send(self->connection->client, (hb_mutation_t)hb_put, put_callback, call_back_buffer);
            //printf("dict ref count after send %i\n", dict->ob_refcnt);
            // TODO ADD the hb_put to the call back buffer and free it!
            CHECK_RC_RETURN(err);
            if (err != 0) {
                pthread_mutex_lock(&batch_call_back_buffer->mutex);
                batch_call_back_buffer->errors++;
                batch_call_back_buffer->count++;
                pthread_mutex_unlock(&batch_call_back_buffer->mutex);

                pthread_mutex_lock(&call_back_buffer->mutex);
                call_back_buffer->count++;
                if (call_back_buffer->err == 0) {
                    call_back_buffer->err = err;
                }
                pthread_mutex_unlock(&call_back_buffer->mutex);
            }
            //printf("hb mutation send was %i\n",err);
        } else if (strcmp(mutation_type_char, "delete") == 0) {
            //printf("its a delete");
            hb_delete_t hb_delete;
            err = make_delete(self, row_key_char, &hb_delete);
            CHECK_RC_RETURN(err);
            if (err != 0) {
                pthread_mutex_lock(&batch_call_back_buffer->mutex);
                batch_call_back_buffer->errors++;
                batch_call_back_buffer->count++;
                pthread_mutex_unlock(&batch_call_back_buffer->mutex);

                call_back_buffer->count++;
                call_back_buffer->err = err;
            }
            err = hb_mutation_send(self->connection->client, (hb_mutation_t)hb_delete, delete_callback, call_back_buffer);
            CHECK_RC_RETURN(err);
            if (err != 0) {
                pthread_mutex_lock(&batch_call_back_buffer->mutex);
                batch_call_back_buffer->errors++;
                batch_call_back_buffer->count++;
                pthread_mutex_unlock(&batch_call_back_buffer->mutex);

                pthread_mutex_lock(&call_back_buffer->mutex);
                call_back_buffer->count++;
                if (call_back_buffer->err == 0) {
                    call_back_buffer->err = err;
                }
                pthread_mutex_unlock(&call_back_buffer->mutex);
            }
        } else {
            // Must be put or delete
            pthread_mutex_lock(&batch_call_back_buffer->mutex);
            batch_call_back_buffer->errors++;
            batch_call_back_buffer->count++;
            pthread_mutex_unlock(&batch_call_back_buffer->mutex);

            call_back_buffer->count++;
            call_back_buffer->err = -1; //TODO BETTER
        }
    }

    printf("done with loop going to flush\n");

    long wait = 0;

    if (number_of_actions > 0) {

        //self->count = 0;
        // TODO Oh no ... The docs say:
        // TODO Note that this doesn't guarantee that ALL outstanding RPCs have completed.
        // TODO Need to figure out the implications of this...
        err = hb_client_flush(self->connection->client, client_flush_callback, NULL);
        if (err != 0) {
            // The documentation doesn't specify if this would ever return an error or why.
            // If this fails with an error and the call back is never invoked, my script would hang..
            // I'll temporarily raise an error until I can clarify this
            PyErr_SetString(PyExc_ValueError, "Flush failed! Not sure if puts have been sent");
            return NULL;
        }
        printf("Waiting for all callbacks to return ...\n");


        //while (self->count < number_of_actions) {
        while (batch_call_back_buffer->count < number_of_actions) {
            // TODO this sleep should be optimized based on the number of actions?
            // E.g. perhaps at most 1 full second is OK if the number of actions is large enough?
            sleep(0.1);
            wait++;
            //printf("wait is %i\n",wait);
            /*
            if (wait > 10000000) {
                //printf("wait is %i\n",wait);
                //printf("Count is %i\n", self->count);
                printf("count is %i wait is %ld\n", self->count, wait);
                PyErr_SetString(SpamError, "Library error");
                return NULL;

            }
            */

        }
    }

    int errors = batch_call_back_buffer->errors;
    PyObject *results = PyList_New(0);
    // todo check decref
    if (!results) {
        // TODO if it fails here, all the results have already been saved...
        // I suppose it would be better to return the errors the the caller
        // If the errors are 0, he can assume everything is fine
        // I suppose this could be solved by initializing at the very start of the method?
        PyErr_SetString(PyExc_ValueError, "Out of memory, batch has been saved though...");
        return NULL;
    }

    if (errors > 0) {
        // TODO I should really go through and get the results and give them back to user
    }

    delete batch_call_back_buffer;
    printf("wait was %ld\n", wait);
    PyObject *ret_tuple = Py_BuildValue("iO", errors, results);
    Py_DECREF(results);
    return ret_tuple;
}


static PyMethodDef Table_methods[] = {
    {"row", (PyCFunction) Table_row, METH_VARARGS, "Gets one row"},
    {"put", (PyCFunction) Table_put, METH_VARARGS, "Puts one row"},
    {"scan", (PyCFunction) Table_scan, METH_VARARGS, "Scans the table"},
    {"delete", (PyCFunction) Table_delete, METH_VARARGS, "Deletes one row"},
    {"batch", (PyCFunction) Table_batch, METH_VARARGS, "sends a batch"},
    {NULL}
};

// Declare the type components
static PyTypeObject TableType = {
   PyObject_HEAD_INIT(NULL)
   0,                         /* ob_size */
   "spam._table",               /* tp_name */
   sizeof(Table),         /* tp_basicsize */
   0,                         /* tp_itemsize */
   (destructor)Table_dealloc, /* tp_dealloc */
   0,                         /* tp_print */
   0,                         /* tp_getattr */
   0,                         /* tp_setattr */
   0,                         /* tp_compare */
   0,                         /* tp_repr */
   0,                         /* tp_as_number */
   0,                         /* tp_as_sequence */
   0,                         /* tp_as_mapping */
   0,                         /* tp_hash */
   0,                         /* tp_call */
   0,                         /* tp_str */
   0,                         /* tp_getattro */
   0,                         /* tp_setattro */
   0,                         /* tp_as_buffer */
   Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /* tp_flags*/
   "Connection object",        /* tp_doc */
   0,                         /* tp_traverse */
   0,                         /* tp_clear */
   0,                         /* tp_richcompare */
   0,                         /* tp_weaklistoffset */
   0,                         /* tp_iter */
   0,                         /* tp_iternext */
   Table_methods,         /* tp_methods */
   Table_members,         /* tp_members */
   0,                         /* tp_getset */
   0,                         /* tp_base */
   0,                         /* tp_dict */
   0,                         /* tp_descr_get */
   0,                         /* tp_descr_set */
   0,                         /* tp_dictoffset */
   (initproc)Table_init,  /* tp_init */
   0,                         /* tp_alloc */
   PyType_GenericNew,                         /* tp_new */
};

/*
struct Connection {
    hb_connection_t conn;
    hb_client_t client;
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

};
*/

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










/*
static PyObject *pymaprdb_get(Connection *connection,const char *table_name, char *row_key) {
    printf("Inside pymaprdb_get\n");
    //RowBuffer *rowBuf = new RowBuffer();
    //char *rk = rowBuf->getBuffer(1024);
    char *rk = connection->rowBuf->getBuffer(1024);
    //char rk[1024];
    // Both of those work^ I wonder why they use the row buffer?

    //printf("Enter key to get: ");
    //scanf("%s", rk);
    strcpy(rk, row_key);
    printf("In test_get after strcpy, rowkey is %s\n", connection->rowBuf->allocedBufs.back());


    //hb_connection_t conn;
    int err = 0;


    //if (!user) {
        //err = hb_connection_create(cldbs, NULL, &conn);
        //CHECK_RC_RETURN(err);
    //} else {
    //    err = hb_connection_create_as_user(cldbs, NULL, user, &conn);
    //    CHECK_RC_RETURN(err);
    //}

    //hb_client_t client;

    //err = hb_client_create(conn, &client);
    //CHECK_RC_RETURN(err);

    hb_get_t get;
    err = hb_get_create((const byte_t *)rk, strlen(rk) + 1, &get);
    CHECK_RC_RETURN(err);

    //err = hb_get_set_table(get, tableName, strlen(tableName));
    err = hb_get_set_table(get, table_name, strlen(table_name));
    CHECK_RC_RETURN(err);


    count = 0;
    //err = hb_get_send(client, get, get_send_cb, rowBuf);
    err = hb_get_send(connection->client, get, get_send_cb, connection->rowBuf);
    CHECK_RC_RETURN(err);

    while (count != 1) { sleep(0.1); }

    hb_client_destroy(connection->client, cl_dsc_cb, connection);

    //sleep(1);
    //hb_connection_destroy(conn);

    printf("Done with test_get\n");
    return connection->rowBuf->ret;
}
*/





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
/*
static PyObject *get(PyObject *self, PyObject *args) {
    char *row_key;
    if (!PyArg_ParseTuple(args, "s", &row_key)) {
        return NULL;
    }
    Connection *connection = new Connection();
    printf("hai I am %s\n", row_key);
    printf("before test_get\n");
    PyObject *lol = pymaprdb_get(connection, tableName, row_key);
    printf("done with foo\n");
    delete connection;
    //noob(row_key);
    return lol;
}
*/
/*
import spam
spam.put('hai', {'Name:First': 'Matthew'})
*/


/*
import spam
spam.scan()
*/

/*


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
*/

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

    if (!PyArg_ParseTuple(args, "O!", &PyDict_Type, &dict)) {
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
    int num;
    if (!PyArg_ParseTuple(args, "i", &num)) {
        return NULL;
    }
    printf("num is %i\n", num);
    PyObject *list = PyList_New(0);
    int i = 0;
    for (i = 0; i < num; i++) {
        PyObject *val = Py_BuildValue("s", "hai");
        PyList_Append(list, val);
        // This doesn't seem to help?
        Py_DECREF(val);
    }

    return list;
}

/*
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

    //char *first = (char *) malloc(1 + 1 + strlen(f1) + strlen(f2));
    //strcpy(first, f1);
    //first[strlen(f1)] = ':';
    //strcat(first, k1);


    // somehow take args as a tuple
    PyObject *dict = PyDict_New();

    char *first = hbase_fqcolumn(f1, k1);
    if (!first) {
            return NULL;//ENOMEM Cannot allocate memory
        }
    char *second = hbase_fqcolumn(f2, k2);
    if (!second) {
            return NULL;//ENOMEM Cannot allocate memory
    }

    printf("First is %s\n", first);
    printf("Second is %s\n", second);

    PyDict_SetItem(dict, Py_BuildValue("s", first), Py_BuildValue("s", v1));
    free(first);
    PyDict_SetItem(dict, Py_BuildValue("s", second), Py_BuildValue("s", v2));
    free(second);

    return dict;
}
*/

static PyObject *print_list(PyObject *self, PyObject *args) {
    //PyListObject seems to suck, it isn't accepted by PyList_Size for example
    PyObject *actions;

    if (!PyArg_ParseTuple(args, "O!", &PyList_Type, &actions)) {
        return NULL;
    }

    //http://effbot.org/zone/python-capi-sequences.htm
    // This guy recommends PySequence_Fast api
    PyObject *value;
    Py_ssize_t i;
    for (i = 0; i < PyList_Size(actions); i++) {
        value = PyList_GetItem(actions, i);
        printf("value is %s\n", PyString_AsString(value));
    }

    Py_RETURN_NONE;
}

/*
import spam
spam.print_list_t([('put', 'row1', {'a':'b'}), ('delete', 'row2')])
*/
static PyObject *print_list_t(PyObject *self, PyObject *args) {
    //PyListObject seems to suck, it isn't accepted by PyList_Size for example
    PyObject *actions;

    if (!PyArg_ParseTuple(args, "O!", &PyList_Type, &actions)) {
        return NULL;
    }

    //http://effbot.org/zone/python-capi-sequences.htm
    // This guy recommends PySequence_Fast api

    PyObject *tuple;
    Py_ssize_t i;
    for (i = 0; i < PyList_Size(actions); i++) {
        tuple = PyList_GetItem(actions, i);
        printf("got tuple\n");
        char *mutation_type = PyString_AsString(PyTuple_GetItem(tuple, 0));
        printf("got mutation_type\n");
        printf("mutation type is %s\n", mutation_type);
        if (strcmp(mutation_type, "put") == 0) {
            printf("Its a put");
        } else if (strcmp(mutation_type, "delete") == 0) {
            printf("its a delete");
        }
    }

    Py_RETURN_NONE;
}

/*
import string
import spam
spam.print_list([c for c in string.letters])
*/
static PyObject *print_list_fast(PyObject *self, PyObject *args) {
    //http://effbot.org/zone/python-capi-sequences.htm
    // This guy says the PySqeunce_Fast api is faster
    // hm later on he says You can also use the PyList API (dead link), but that only works for lists, and is only marginally faster than the PySequence_Fast API.
    PyObject *actions;

    if (!PyArg_ParseTuple(args, "O!", &PyList_Type, &actions)) {
        return NULL;
    }

    PyObject *seq;
    int i, len;

    PyObject *value;

    seq = PySequence_Fast(actions, "expected a sequence");
    len = PySequence_Size(actions);

    for (i = 0; i < len; i++) {
        value = PySequence_Fast_GET_ITEM(seq, i);
        printf("Value is %s\n", PyString_AsString(value));
    }

    Py_RETURN_NONE;


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
    int lol = pymaprdb_get(NULL);
    Py_RETURN_NONE;
}
*/

static PyMethodDef SpamMethods[] = {
    {"system",  spam_system, METH_VARARGS, "Execute a shell command."},
    {"lol", lol, METH_VARARGS, "your a lol"},
    //{"get", get, METH_VARARGS, "gets a row given a rowkey"},
    //{"put", put, METH_VARARGS, "puts a row and dict"},
    //{"scan", scan, METH_VARARGS, "scans"},
    {"build_int", build_int, METH_VARARGS, "build an int"},
    {"build_dict", build_dict, METH_VARARGS, "build a dict"},
    {"add_to_dict", add_to_dict, METH_VARARGS, "add to dict"},
    //{"super_dict", super_dict, METH_VARARGS, "super dict"},
    {"print_dict", print_dict, METH_VARARGS, "print dict"},
    {"build_list", build_list, METH_VARARGS, "build list"},
    {"print_list", print_list, METH_VARARGS, "prints a list"},
    {"print_list_fast", print_list_fast, METH_VARARGS, "prints a list using the fast api"},
    {"print_list_t", print_list_t, METH_VARARGS, "pritns a list of tuples"},
    {NULL, NULL, 0, NULL}
};

#ifndef PyMODINIT_FUNC	/* declarations for DLL import/export */
#define PyMODINIT_FUNC void
#endif
PyMODINIT_FUNC
initspam(void)
{
    PyObject *m;

    m = Py_InitModule("spam", SpamMethods);
    if (m == NULL) {
        return;
    }

    // Fill in some slots in the type and make it ready
    // I suppose I use this if I don't write my own new mthod?
    //FooType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&FooType) < 0) {
        return;
    }

    if (PyType_Ready(&ConnectionType) < 0) {
        return;
    }

    if (PyType_Ready(&TableType) < 0) {
        return;
    }


    // no tp_new here because its in the FooType
    Py_INCREF(&FooType);
    PyModule_AddObject(m, "Foo", (PyObject *) &FooType);

    // Add the type to the module
    // failing to add this tp_new will result in: TypeError: cannot create 'spam._connection' instances
    ConnectionType.tp_new = PyType_GenericNew;
    Py_INCREF(&ConnectionType);
    PyModule_AddObject(m, "_connection", (PyObject *) &ConnectionType);

    //TableType.tp_new = PyType_GenericNew;
    Py_INCREF(&TableType);
    PyModule_AddObject(m, "_table", (PyObject *) &TableType);

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
