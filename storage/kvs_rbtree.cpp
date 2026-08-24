
#include "../kvstore.h"
#include "kvs_rbtree.h"
#include <cstring>
// #include <stdio.h>
// #include <stdlib.h>
// #include <string.h>
kvs_rbtree_t global_rbtree = {0};

rbtree_node* rbtree_mini(rbtree* T, rbtree_node* x) {
    while (x->left != T->nil) {
        x = x->left;
    }
    return x;
}

rbtree_node* rbtree_maxi(rbtree* T, rbtree_node* x) {
    while (x->right != T->nil) {
        x = x->right;
    }
    return x;
}

rbtree_node* rbtree_successor(rbtree* T, rbtree_node* x) {
    rbtree_node* y = x->parent;

    if (x->right != T->nil) {
        return rbtree_mini(T, x->right);
    }

    while ((y != T->nil) && (x == y->right)) {
        x = y;
        y = y->parent;
    }
    return y;
}

void rbtree_left_rotate(rbtree* T, rbtree_node* x) {

    rbtree_node* y = x->right; // x  --> y  ,  y --> x,   right --> left,  left --> right

    x->right = y->left;      // 1 1
    if (y->left != T->nil) { // 1 2
        y->left->parent = x;
    }

    y->parent = x->parent;     // 1 3
    if (x->parent == T->nil) { // 1 4
        T->root = y;
    } else if (x == x->parent->left) {
        x->parent->left = y;
    } else {
        x->parent->right = y;
    }

    y->left = x;   // 1 5
    x->parent = y; // 1 6
}

void rbtree_right_rotate(rbtree* T, rbtree_node* y) {

    rbtree_node* x = y->left;

    y->left = x->right;
    if (x->right != T->nil) {
        x->right->parent = y;
    }

    x->parent = y->parent;
    if (y->parent == T->nil) {
        T->root = x;
    } else if (y == y->parent->right) {
        y->parent->right = x;
    } else {
        y->parent->left = x;
    }

    x->right = y;
    y->parent = x;
}

void rbtree_insert_fixup(rbtree* T, rbtree_node* z) {

    while (z->parent->color == RED) { // z ---> RED
        if (z->parent == z->parent->parent->left) {
            rbtree_node* y = z->parent->parent->right;
            if (y->color == RED) {
                z->parent->color = BLACK;
                y->color = BLACK;
                z->parent->parent->color = RED;

                z = z->parent->parent; // z --> RED
            } else {

                if (z == z->parent->right) {
                    z = z->parent;
                    rbtree_left_rotate(T, z);
                }

                z->parent->color = BLACK;
                z->parent->parent->color = RED;
                rbtree_right_rotate(T, z->parent->parent);
            }
        } else {
            rbtree_node* y = z->parent->parent->left;
            if (y->color == RED) {
                z->parent->color = BLACK;
                y->color = BLACK;
                z->parent->parent->color = RED;

                z = z->parent->parent; // z --> RED
            } else {
                if (z == z->parent->left) {
                    z = z->parent;
                    rbtree_right_rotate(T, z);
                }

                z->parent->color = BLACK;
                z->parent->parent->color = RED;
                rbtree_left_rotate(T, z->parent->parent);
            }
        }
    }

    T->root->color = BLACK;
}

void rbtree_insert(rbtree* T, rbtree_node* z) {

    rbtree_node* y = T->nil;
    rbtree_node* x = T->root;

    while (x != T->nil) {
        y = x;
#if ENABLE_KEY_CHAR
        if (strcmp(z->key, x->key) < 0) {
            x = x->left;
        } else if (strcmp(z->key, x->key) > 0) {
            x = x->right;
        } else { // Exist
            return;
        }
#else
        if (z->key < x->key) {
            x = x->left;
        } else if (z->key > x->key) {
            x = x->right;
        } else { // Exist
            return;
        }
#endif
    }

#if ENABLE_KEY_CHAR

    z->parent = y;
    if (y == T->nil) {
        T->root = z;
    } else if (strcmp(z->key, y->key) < 0) {
        y->left = z;
    } else {
        y->right = z;
    }
#else
    z->parent = y;
    if (y == T->nil) {
        T->root = z;
    } else if (z->key < y->key) {
        y->left = z;
    } else {
        y->right = z;
    }
#endif

    z->left = T->nil;
    z->right = T->nil;
    z->color = RED;

    rbtree_insert_fixup(T, z);
}

void rbtree_delete_fixup(rbtree* T, rbtree_node* x) {

    while ((x != T->root) && (x->color == BLACK)) {
        if (x == x->parent->left) {

            rbtree_node* w = x->parent->right;
            if (w->color == RED) {
                w->color = BLACK;
                x->parent->color = RED;

                rbtree_left_rotate(T, x->parent);
                w = x->parent->right;
            }

            if ((w->left->color == BLACK) && (w->right->color == BLACK)) {
                w->color = RED;
                x = x->parent;
            } else {

                if (w->right->color == BLACK) {
                    w->left->color = BLACK;
                    w->color = RED;
                    rbtree_right_rotate(T, w);
                    w = x->parent->right;
                }

                w->color = x->parent->color;
                x->parent->color = BLACK;
                w->right->color = BLACK;
                rbtree_left_rotate(T, x->parent);

                x = T->root;
            }

        } else {

            rbtree_node* w = x->parent->left;
            if (w->color == RED) {
                w->color = BLACK;
                x->parent->color = RED;
                rbtree_right_rotate(T, x->parent);
                w = x->parent->left;
            }

            if ((w->left->color == BLACK) && (w->right->color == BLACK)) {
                w->color = RED;
                x = x->parent;
            } else {

                if (w->left->color == BLACK) {
                    w->right->color = BLACK;
                    w->color = RED;
                    rbtree_left_rotate(T, w);
                    w = x->parent->left;
                }

                w->color = x->parent->color;
                x->parent->color = BLACK;
                w->left->color = BLACK;
                rbtree_right_rotate(T, x->parent);

                x = T->root;
            }
        }
    }

    x->color = BLACK;
}

rbtree_node* rbtree_delete(rbtree* T, rbtree_node* z) {

    rbtree_node* y = T->nil;
    rbtree_node* x = T->nil;

    if ((z->left == T->nil) || (z->right == T->nil)) {
        y = z;
    } else {
        y = rbtree_successor(T, z);
    }

    if (y->left != T->nil) {
        x = y->left;
    } else if (y->right != T->nil) {
        x = y->right;
    }

    x->parent = y->parent;
    if (y->parent == T->nil) {
        T->root = x;
    } else if (y == y->parent->left) {
        y->parent->left = x;
    } else {
        y->parent->right = x;
    }

    if (y != z) {
#if 1
        KEY_TYPE tmp_key = z->key;
        z->key = y->key;
        y->key = tmp_key;

        void* tmp_val = z->value;
        z->value = y->value;
        y->value = tmp_val;

#elif ENABLE_KEY_CHAR
        void* temp = z->key;
        z->key = y->key;
        y->key = (KEY_TYPE)temp;

        temp = z->value;
        z->value = y->value;
        y->value = temp;

#else
        z->key = y->key;
        z->value = y->value;
#endif
    }

    if (y->color == BLACK) {
        rbtree_delete_fixup(T, x);
    }

    return y;
}

//若查找的key不存在，不会返回 nullptr! 不会返回nullptr! 不会返回nullptr!
//会返回T->nil
rbtree_node* rbtree_search(rbtree* T, KEY_TYPE key) {

    rbtree_node* node = T->root;
    while (node != T->nil) {
#if ENABLE_KEY_CHAR
        if (strcmp(key, node->key) < 0) {
            node = node->left;
        } else if (strcmp(key, node->key) > 0) {
            node = node->right;
        } else {
            return node;
        }
#else
        if (key < node->key) {
            node = node->left;
        } else if (key > node->key) {
            node = node->right;
        } else {
            return node;
        }
#endif
    }
    return T->nil;
}

void rbtree_traversal(rbtree* T, rbtree_node* node) {
    if (node != T->nil) {
        rbtree_traversal(T, node->left);
#if ENABLE_KEY_CHAR
        printf("key:%s, color:%d\n", node->key, node->color);
#else
        printf("key:%d, color:%d\n", node->key, node->color);
#endif
        rbtree_traversal(T, node->right);
    }
}

#if 0
int main() {

#if ENABLE_KEY_CHAR
    KEY_TYPE keyArray[20] = {"K", "C", "D", "T", "E", "A", "B", "J", "L", "F",
                             "O", "P", "Q", "R", "S", "G", "H", "I", "M", "N"};
#else
    int keyArray[20] = {24, 25, 13, 35, 23, 26, 67, 47, 38, 98,
                        20, 19, 17, 49, 12, 21, 9,  18, 14, 15};
#endif

    rbtree* T = (rbtree*)malloc(sizeof(rbtree));
    if (T == NULL) {
        printf("malloc failed\n");
        return -1;
    }

    T->nil = (rbtree_node*)malloc(sizeof(rbtree_node));
    T->nil->color = BLACK;
    T->root = T->nil;

    rbtree_node* node = T->nil;
    int i = 0;
    for (i = 0; i < 20; i++) {
        node = (rbtree_node*)malloc(sizeof(rbtree_node));

#if ENABLE_KEY_CHAR
        node->key = (KEY_TYPE)malloc(strlen(keyArray[i]) + 1);
        memcpy(node->key, keyArray[i], strlen(keyArray[i]) + 1);
#else
        node->key = (KEY_TYPE)keyArray[i];
#endif
        node->value = NULL;

        rbtree_insert(T, node);
    }

    rbtree_traversal(T, T->root);
    printf("----------------------------------------\n");

    for (i = 0; i < 20; i++) {

        rbtree_node* node = rbtree_search(T, (KEY_TYPE)keyArray[i]);
        rbtree_node* cur = rbtree_delete(T, node);
        free(cur);

        rbtree_traversal(T, T->root);
        printf("----------------------------------------\n");
    }
}

#else

int kvs_rbtree_create(kvs_rbtree_t* inst) {
    inst->nil = (rbtree_node*)kvs_malloc(sizeof(rbtree_node));
    inst->nil->color = BLACK;
    inst->root = inst->nil;
    return 0;
}

void kvs_rbtree_destroy(kvs_rbtree_t* inst) {
    if (inst == NULL)
        return;
    rbtree_node* node = NULL;
    while (node != inst->root) {
        rbtree_node* mini = rbtree_mini(inst, node);
        rbtree_node* cur = rbtree_delete(inst, mini);
        kvs_free(cur);
    }
    kvs_free(inst->nil);
}

// void kvs_rbtree_destroy(kvs_rbtree_t* inst) {
//     if (inst == NULL) return;
//     while (inst->root != NULL) {
//         rbtree_node* mini = rbtree_mini(inst, inst->root);
//         rbtree_node* removed = rbtree_delete(inst, mini);
//         kvs_free(removed);
//     }
//     kvs_free(inst->nil);
//     inst->nil = NULL;   // 可选，避免野指针
// }

/*
 *@return: (char*)ptr, exist; nullptr, no exist
 */
char* kvs_rbtree_get(kvs_rbtree_t* inst, char* key) {
    if (inst == nullptr || key == nullptr)
        return nullptr;

    rbtree_node* node = rbtree_search(inst, key);
    if (node == inst->nil)
        return nullptr;
    return (char*)node->value;
}

/*
 *@return: = 0, success; <0, error; >0, exist
 */
int kvs_rbtree_set(kvs_rbtree_t* inst, char* key, char* value) {
    if (inst == nullptr || key == nullptr || value == nullptr) {
        return -1;
    }

    rbtree_node* node = rbtree_search(inst, key);
    if (node != inst->nil)
        return 1;

    node = (rbtree_node*)kvs_malloc(sizeof(rbtree_node));
    node->key = (char*)kvs_malloc(strlen(key) + 1);
    if (node->key == nullptr)
        return -2;
    memcpy(node->key, key, strlen(key) + 1);

    node->value = kvs_malloc(strlen(value) + 1);
    if (node->value == nullptr)
        return -2;
    memcpy(node->value, value, strlen(value) + 1);

    rbtree_insert(inst, node);
    return 0;
}

/*
 *@return: = 0, success; <0, error; >0, no exist
 */
int kvs_rbtree_del(kvs_rbtree_t* inst, char* key) {
    if (inst == nullptr || key == nullptr)
        return -1;

    rbtree_node* node = rbtree_search(inst, key);
    if (node == inst->nil)
        return 1;
    rbtree_node* cur = rbtree_delete(inst, node);
    kvs_free(cur);
    return 0;
}

/*
 *@return:  = 0, success; < 0, error; > 0 no exist
 */
int kvs_rbtree_mod(kvs_rbtree_t* inst, char* key, char* value) {
    if (inst == nullptr || key == nullptr || value == nullptr)
        return -1;

    rbtree_node* node = rbtree_search(inst, key);
    if (node == inst->nil)
        return 1;
    char* str = (char*)kvs_malloc(strlen(value) + 1);
    if (str == nullptr)
        return -2;
    memcpy(str, value, strlen(value) + 1);

    char* temp = (char*)node->value;
    node->value = str;
    kvs_free(temp);

    return 0;
}

/*
 *@return: = 0, exist; < 0, error; > 0 no exist
 */
int kvs_rbtree_exist(kvs_rbtree_t* inst, char* key) {
    if (inst == nullptr || key == nullptr)
        return -1;

    rbtree_node* node = rbtree_search(inst, key);

    if (node == inst->nil)
        return 1;

    return 0;
}

#endif
