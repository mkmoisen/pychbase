struct Buffer {
    int count;
    pthread_mutex_t mutex;

    Buffer() {
        mutex = PTHREAD_MUTEX_INITIALIZER;
    }
};

void call_back(Buffer *buffer) {
    //...
    // Assume that only 1 thread has write access
    buffer->count = 1;
}

void caller() {
    Buffer *buffer = new Buffer()
    //...
    some_async_function_with_thread(call_back, buffer);
    // Assume that only 1 thread will be reading like this:
    while (buffer->count != 1) {
        sleep(0.1);
    }
    delete buffer;
}