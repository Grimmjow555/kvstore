

#include "kvstore.h"
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>

kvs_skiptable_t global_skiptable;

#if HASH_ENABLE_CHAR_KV
Node* createNode(int level, const char* key, const char* value) {
    // 1. 分配节点结构体
    Node* newNode = (Node*)kvs_malloc(sizeof(Node));
    if (newNode == NULL)
        return NULL;

    // 2. 复制键（如果 key 非空）
    if (key != NULL) {
        newNode->key = (char*)kvs_malloc(strlen(key) + 1);
        if (newNode->key == NULL) {
            kvs_free(newNode);
            return NULL;
        }
        memcpy(newNode->key, key, strlen(key) + 1);
    } else {
        newNode->key = NULL; // 头节点使用
    }

    // 3. 复制值（如果 value 非空）
    if (value != NULL) {
        newNode->value = (char*)kvs_malloc(strlen(value) + 1);
        if (newNode->value == NULL) {
            kvs_free(newNode->key); // 释放已分配的键
            kvs_free(newNode);
            return NULL;
        }
        memcpy(newNode->value, value, strlen(value) + 1);
    } else {
        newNode->value = NULL;
    }

    // 4. 分配 forward 指针数组
    newNode->forward = (Node**)kvs_malloc((level + 1) * sizeof(Node*));
    if (newNode->forward == NULL) {
        kvs_free(newNode->value);
        kvs_free(newNode->key);
        kvs_free(newNode);
        return NULL;
    }

    // 5. 将所有 forward 指针初始化为 NULL（安全习惯）
    for (int i = 0; i <= level; ++i) {
        newNode->forward[i] = NULL;
    }

    return newNode;
}
#else
Node* createNode(int level, int key, int value) {
    Node* newNode = (Node*)kvs_malloc(sizeof(Node));
    newNode->key = key;
    newNode->value = value;
    newNode->forward = (Node**)kvs_malloc((level + 1) * sizeof(Node*));

    return newNode;
}
#endif

#if HASH_ENABLE_CHAR_KV
SkipList* createSkipList() {
    SkipList* skipList = (SkipList*)kvs_malloc(sizeof(SkipList));
    skipList->level = 0;

    skipList->header = createNode(MAX_LEVEL, nullptr, nullptr);

    for (int i = 0; i <= MAX_LEVEL; ++i) {
        skipList->header->forward[i] = NULL;
    }

    return skipList;
}
#else
SkipList* createSkipList() {
    SkipList* skipList = (SkipList*)kvs_malloc(sizeof(SkipList));
    skipList->level = 0;

    skipList->header = createNode(MAX_LEVEL, -1, -1);

    for (int i = 0; i <= MAX_LEVEL; ++i) {
        skipList->header->forward[i] = NULL;
    }

    return skipList;
}
#endif

int randomLevel() {
    int level = 0;
    while (rand() < RAND_MAX / 2 && level < MAX_LEVEL)
        level++;
    return level;
}

#if HASH_ENABLE_CHAR_KV
bool insert(SkipList* skipList, char* key, char* value) {
    Node* update[MAX_LEVEL + 1];
    Node* current = skipList->header;

    for (int i = skipList->level; i >= 0; --i) {
        while (current->forward[i] != NULL && strcmp(current->forward[i]->key, key) < 0)
            current = current->forward[i];
        update[i] = current;
    }

    current = current->forward[0];

    if (current == NULL || strcmp(current->key, key) != 0) {
        int level = randomLevel();

        if (level > skipList->level) {
            for (int i = skipList->level + 1; i <= level; ++i)
                update[i] = skipList->header;
            skipList->level = level;
        }

        Node* newNode = createNode(level, key, value);

        for (int i = 0; i <= level; ++i) {
            newNode->forward[i] = update[i]->forward[i];
            update[i]->forward[i] = newNode;
        }

        printf("Inserted key %s\n", key);

        return true;
    } else {
        printf("Key %s already exists\n", key);
        return false;
    }
}
#else
bool insert(SkipList* skipList, int key, int value) {
    Node* update[MAX_LEVEL + 1];
    Node* current = skipList->header;

    for (int i = skipList->level; i >= 0; --i) {
        while (current->forward[i] != NULL && current->forward[i]->key < key)
            current = current->forward[i];
        update[i] = current;
    }

    current = current->forward[0];

    if (current == NULL || current->key != key) {
        int level = randomLevel();

        if (level > skipList->level) {
            for (int i = skipList->level + 1; i <= level; ++i)
                update[i] = skipList->header;
            skipList->level = level;
        }

        Node* newNode = createNode(level, key, value);

        for (int i = 0; i <= level; ++i) {
            newNode->forward[i] = update[i]->forward[i];
            update[i]->forward[i] = newNode;
        }

        printf("Inserted key %d\n", key);

        return true;
    } else {
        printf("Key %d already exists\n", key);
        return false;
    }
}
#endif

#if HASH_ENABLE_CHAR_KV
void display(SkipList* skipList) {
    printf("Skip List:\n");

    for (int i = 0; i <= skipList->level; ++i) {
        Node* node = skipList->header->forward[i];
        printf("Level %d: ", i);

        while (node != NULL) {
            printf("%s ", node->key);
            node = node->forward[i];
        }

        printf("\n");
    }
}
#else
void display(SkipList* skipList) {
    printf("Skip List:\n");

    for (int i = 0; i <= skipList->level; ++i) {
        Node* node = skipList->header->forward[i];
        printf("Level %d: ", i);

        while (node != NULL) {
            printf("%d ", node->key);
            node = node->forward[i];
        }

        printf("\n");
    }
}
#endif

#if HASH_ENABLE_CHAR_KV
bool search(SkipList* skipList, char* key) {
    Node* current = skipList->header;

    for (int i = skipList->level; i >= 0; --i) {
        while (current->forward[i] != NULL && strcmp(current->forward[i]->key, key) < 0)
            current = current->forward[i];
    }

    current = current->forward[0];

    if (current && strcmp(current->key, key) == 0) {
        printf("Key %s found with value %s\n", key, current->value);
        return true;
    } else {
        printf("Key %s not found\n", key);
        return false;
    }
}
#else
bool search(SkipList* skipList, int key) {
    Node* current = skipList->header;

    for (int i = skipList->level; i >= 0; --i) {
        while (current->forward[i] != NULL && current->forward[i]->key < key)
            current = current->forward[i];
    }

    current = current->forward[0];

    if (current && current->key == key) {
        printf("Key %d found with value %d\n", key, current->value);
        return true;
    } else {
        printf("Key %d not found\n", key);
        return false;
    }
}
#endif

#if 0
int main() {
    SkipList* skipList = createSkipList();

    insert(skipList, "WYH", "0");
    insert(skipList, "A", "1");
    insert(skipList, "B", "2");
    insert(skipList, "C", "3");

    display(skipList);

    search(skipList, "?");
    search(skipList, "A");

    return 0;
}

#else

int kvs_skiptable_create(kvs_skiptable_t* skiptable) {
    if (skiptable == nullptr)
        return -1;
    skiptable->level = 0;

    Node* header = (Node*)kvs_malloc(sizeof(Node));
    if (header == nullptr)
        return -1;
    header->key = nullptr;

    header->value = nullptr;

    header->forward = (Node**)kvs_malloc((MAX_LEVEL + 1) * sizeof(Node*));
    if (header->forward == nullptr) {
        kvs_free(header);
        return -1;
    }

    for (int i = 0; i <= MAX_LEVEL; ++i) {
        header->forward[i] = NULL;
    }
    skiptable->header = header;

    return 0;
}

void kvs_skiptable_destroy(kvs_skiptable_t* skiptable) {
    if (skiptable == NULL)
        return;

    // 从最底层（Level 0）遍历所有节点，逐个释放
    Node* current = skiptable->header->forward[0];
    while (current != NULL) {
        Node* temp = current;
        current = current->forward[0];
        kvs_free(temp->forward); // 释放 forward 数组
        kvs_free(temp);          // 释放节点本身
    }

    // 释放头节点
    kvs_free(skiptable->header->forward);
    kvs_free(skiptable->header);
}

int kvs_skiptable_set(kvs_skiptable_t* skiptable, char* key, char* value) {

    if (skiptable == nullptr || key == nullptr || value == nullptr)
        return -1;

    Node* update[MAX_LEVEL + 1];
    Node* current = skiptable->header;

    for (int i = skiptable->level; i >= 0; --i) {
        while (current->forward[i] != NULL && strcmp(current->forward[i]->key, key) < 0)
            current = current->forward[i];
        update[i] = current;
    }

    current = current->forward[0];

    if (current == NULL || strcmp(current->key, key) != 0) {
        int level = randomLevel();

        if (level > skiptable->level) {
            for (int i = skiptable->level + 1; i <= level; ++i)
                update[i] = skiptable->header;
            skiptable->level = level;
        }

        Node* newNode = (Node*)kvs_malloc(sizeof(Node));
        if (newNode == nullptr)
            return -1;
        newNode->key = (char*)kvs_malloc(strlen(key) + 1);
        if (newNode->key == nullptr)
            return -1;
        memcpy(newNode->key, key, strlen(key) + 1);

        newNode->value = (char*)kvs_malloc(strlen(value) + 1);
        if (newNode->value == nullptr)
            return -1;
        memcpy(newNode->value, value, strlen(value) + 1);

        newNode->forward = (Node**)kvs_malloc((level + 1) * sizeof(Node*));
        if (newNode->forward == nullptr)
            return -1;

        for (int i = 0; i <= level; ++i) {
            newNode->forward[i] = update[i]->forward[i];
            update[i]->forward[i] = newNode;
        }

        return 0;
    } else {
        return 1;
    }
}

char* kvs_skiptable_get(kvs_skiptable_t* skiptable, char* key) {

    if (skiptable == nullptr || key == nullptr)
        return nullptr;

    Node* current = skiptable->header;

    for (int i = skiptable->level; i >= 0; --i) {
        while (current->forward[i] != NULL && strcmp(current->forward[i]->key, key) < 0)
            current = current->forward[i];
    }

    current = current->forward[0];

    if (current && strcmp(current->key, key) == 0) {
        return current->value;
    } else {
        return nullptr;
    }
}

int kvs_skiptable_mod(kvs_skiptable_t* skiptable, char* key, char* value) {
    if (skiptable == nullptr || key == nullptr || value == nullptr)
        return -1;

    Node* current = skiptable->header;

    for (int i = skiptable->level; i >= 0; --i) {
        while (current->forward[i] != NULL && strcmp(current->forward[i]->key, key) < 0)
            current = current->forward[i];
    }

    current = current->forward[0];

    if (current && strcmp(current->key, key) == 0) {
        char* new_value = (char*)kvs_malloc(strlen(value) + 1);
        if (new_value == nullptr)
            return -1;
        memcpy(new_value, value, strlen(value) + 1);

        kvs_free(current->value);
        current->value = new_value;

        return 0;
    } else {
        return 1;
    }
}

int kvs_skiptable_del(kvs_skiptable_t* skiptable, char* key) {
    if (skiptable == nullptr || key == nullptr)
        return -1;

    Node* current = skiptable->header;
    Node* update[MAX_LEVEL + 1];
    // 1. 从最高层向下查找，记录每层的前驱节点
    for (int i = skiptable->level; i >= 0; --i) {
        while (current->forward[i] != NULL && strcmp(current->forward[i]->key, key) < 0)
            current = current->forward[i];
        update[i] = current;
    }

    // 2. 到达最底层，检查下一个节点是否就是要删除的键
    current = current->forward[0];
    if (current == NULL || strcmp(current->key, key) != 0) {
        return 1; // 未找到
    }

    // 3. 找到了，逐层删除该节点
    for (int i = 0; i <= skiptable->level; ++i) {
        if (update[i]->forward[i] == current) {
            update[i]->forward[i] = current->forward[i]; // 跳过当前节点
        }
    }

    // 4. 释放节点内部资源
    kvs_free(current->key);
    kvs_free(current->value);
    kvs_free(current->forward); // 释放 forward 指针数组
    kvs_free(current);

    // 5. （可选）调整跳表层数：如果最高层只剩头节点，则降低 level
    while (skiptable->level > 0 && skiptable->header->forward[skiptable->level] == NULL) {
        skiptable->level--;
    }

    return 0; // 成功删除
}

int kvs_skiptable_exist(kvs_skiptable_t* skiptable, char* key) {
    if (skiptable == nullptr || key == nullptr)
        return -1;

    Node* current = skiptable->header;

    for (int i = skiptable->level; i >= 0; --i) {
        while (current->forward[i] != NULL && strcmp(current->forward[i]->key, key) < 0)
            current = current->forward[i];
    }

    current = current->forward[0];

    if (current && strcmp(current->key, key) == 0) {
        return 0;
    } else {
        return 1;
    }
}
#endif
