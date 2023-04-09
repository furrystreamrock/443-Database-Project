#include "memtab.cpp"

const int max_keys = 2; //B

struct BTNode {
    BTNode* parent;
    bool is_leaf;

    int num_keys;
    int keys[max_keys];
    kv_pair* pairs[max_keys];

    int num_children;
    BTNode* child[max_keys + 1];

    BTNode()
    {
        this->num_keys = 0;
        this->is_leaf = true;
        this->parent = nullptr;
    }
};

class btree {
	private:
		BTNode* root;
		int entries;

        bool search_step(BTNode* node, int key, BTNode** node_found, int* child_i){
			for (int i = 0; i < node->num_keys; i++) {
                if (key == node->keys[i]){
                    *node_found = node;
                    *child_i = i;
                    return true;
                } else if (key < node->keys[i]){
                    if (node->is_leaf == true){
                        *node_found = node;
                        *child_i = i;
                        return false;
                    }
                    return search_step( (BTNode*)(node->child[i]) , key, node_found, child_i);
                } else if (i == node->num_keys - 1){
                    if (node->is_leaf == true){
                        *node_found = node;
                        *child_i = i + 1;
                        return false;
                    }
                    return search_step( (BTNode*)(node->child[i + 1]) , key, node_found, child_i);
                }
            }
        }
        
        btree* clear(BTNode* node){
            if (node->is_leaf) {
                //does not delete kv_pairs
                delete node;
                return;
            }
			for (int i = 0; i < node->num_children; i++) {
                clear((BTNode*)(node->child[i]));
            }
        }
        
        void find_lowest_oe(BTNode* node, int min, kv_pair** min_pair, bool* found_valid){
            
			for (int i = 0; i < node->num_keys; i++) {
                if (node->keys[i] == min) {
                    *min_pair = node->pairs[i];
                    *found_valid = true;
                    return;
                } else if (node->keys[i] > min) {
                    *min_pair = node->pairs[i];
                    *found_valid = true;
                    if (node->is_leaf == false) {
                        return find_lowest_oe(node->child[i], min, min_pair, found_valid);
                    }
                }
            }

            if (node->child[max_keys + 1] != nullptr) {
                return find_lowest_oe(node->child[max_keys + 1], min, min_pair, found_valid);
            }
        }
        void find_highest_ue(BTNode* node, int max, kv_pair** max_pair, bool* found_valid){

			for (int i = 0; i < node->num_keys; i++) {
                if (node->keys[i] == max) {
                    *max_pair = node->pairs[i];
                    *found_valid = true;
                    return;
                } else if (node->keys[i] < max) {
                    *max_pair = node->pairs[i];
                    *found_valid = true;
                    if (node->is_leaf == false) {
                        return find_highest_ue(node->child[i], max, max_pair, found_valid);
                    }
                }
            }

            if (node->child[max_keys + 1] != nullptr) {
                return find_highest_ue(node->child[max_keys + 1], max, max_pair, found_valid);
            }
        }
	public:


        bool scan(int min, int max, kv_pair** min_pair, kv_pair** max_pair){
            BTNode* result_node;
            int index_found;

            bool found_valid = false;
            find_lowest_oe(root, min, min_pair, &found_valid);
            if (found_valid == false) {
                return false;
            }
            found_valid = false;
            find_highest_ue(root, max, max_pair, &found_valid);
            if (found_valid == false) {
                return false;
            }

            return true;
        }

        bool get(int key, int* val){
            BTNode* result_node;
            int index_found;
            bool found = search_step(root, key, &result_node, &index_found);
            if (found) {
                kv_pair* result_pair = (kv_pair*)(result_node->child[index_found]);
                *val = result_pair->value;
            }
            return found;
        }

        void insert_step(BTNode* node, kv_pair* pair, BTNode* new_right) {
            if (node->num_keys < max_keys) {
			    //you can just insert at the end because the given array is sorted
                node->keys[node->num_keys] = pair->key;
                if (node->is_leaf){
                    node->pairs[node->num_keys] = pair;
                } else {
                    BTNode* place_target;
                    int index_found = node->num_keys;
                    //search_step(node, pair->key, &place_target, &index_found);
                    place_target->pairs[index_found] = pair;

                    if (new_right != nullptr){
                        new_right->parent = node;
                        place_target->child[index_found + 1] = new_right;
                        node->num_children++;
                    }
                }
                node->num_keys++;
            } else {
                //split
                int pivot = ceil(max_keys + 1 / 2);

                BTNode* right = new BTNode();
                int j = 0;
                for (int i = pivot + 1; i < max_keys + 1; i++) {
                    kv_pair* p;
                    BTNode* c;
                    if (i == max_keys) {
                        p = pair;
                        c = node->child[i + 1];
                    } else {
                        p = node->pairs[i];
                        c = node->child[i];
                    }
                    right->keys[j] = p->key;
                    right->pairs[j] = p;
                    right->child[j] = c;
                    right->num_keys++;
                    right->num_children++;
                    if (node->is_leaf){
                        right->is_leaf = true;
                    }

                    if (node->parent != nullptr) {
                        //bubble up
                        insert_step(node->parent, node->pairs[pivot], right);
                    } else {
                        //this is the new root
                        BTNode* new_root = new BTNode();
                        new_root->keys[0] = node->keys[pivot];
                        new_root->pairs[0] = node->pairs[pivot];
                        new_root->num_keys++;

                        new_root->child[0] = node;
                        new_root->child[1] = right;
                        node->parent = new_root;
                        right->parent = new_root;

                        new_root->is_leaf = false;

                        this->root = new_root;
                    }

                    j++;
                }

                if (new_right) {
                    right->child[j] = new_right;
                    right->num_children++;
                }

                    
                node->num_keys = pivot;
                node->num_children = pivot + 1;
            }
        }

        void insert(kv_pair* pair) {
            if (this->root == nullptr) {
                this->root = new BTNode();
                this->root->pairs[0] = pair;
                this->root->num_keys++;
                return;
            }

            BTNode* node;
            int index_found;
            bool found = search_step(this->root, pair->key, &node, &index_found);

            insert_step(node, pair, nullptr);
        }
        
        btree(){
			this->entries = 0;
			this->root = nullptr;
        }
        


        void treeify(kv_pair* pairs, int len){
            for (int i = 0; i < len; i++) {
                insert(&(pairs[i]));
            }
        }

        btree* clear_all(){
            clear(root);
        }

};

