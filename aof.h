#define AOF_ENABLE 1

int kvs_aof_init(const char* filename);
int kvs_aof_append(int argc, char* argv[]);
int kvs_aof_close();
int kvs_aof_replay(const char* filename);
int kvs_aof_is_replaying();
int kvs_aof_clear();