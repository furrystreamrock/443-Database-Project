#include <cmath>
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
        this->num_children = 0;
        this->is_leaf = true;
        this->parent = nullptr;

        for (int i = 0; i < max_keys + 1; i++) {
            this->child[i] = nullptr;
        }
    }
};

class btree {
	private:
		BTNode* root;
		int entries;

        bool search_step(BTNode* node, int key, BTNode** node_found, int* child_i){
            std::cout << "searchx: " << std::endl;
			for (int i = 0; i < node->num_keys; i++) {
                std::cout << "key: " << node->keys[i];
                std::cout << "target: " << key << std::endl;
                if (key == node->keys[i]){
                    *node_found = node;
                    *child_i = i;
                    return true;
                } else if (key < node->keys[i]){
                    std::cout << "yess: " << std::endl;
                    if (node->is_leaf == true){
                        std::cout << "WHAT: " << std::endl;
                        std::cout << "children: " << node->num_children << std::endl;
                        *node_found = node;
                        *child_i = i;
                        return false;
                    }
                    return search_step( node->child[i] , key, node_found, child_i);
                } else if (i == node->num_keys - 1){
                    std::cout << "check rightmost: " << std::endl;
                    if (node->is_leaf == true){
                        std::cout << "fail1: " << std::endl;
                        *node_found = node;
                        *child_i = i + 1;
                        return false;
                    }
                    if (node->child[i + 1] == nullptr) {
                        std::cout << "fail2: " << std::endl;
                    } else if (node->num_children > i + 1) {
                        std::cout << "fail3: " << std::endl;
                        return search_step( node->child[i + 1] , key, node_found, child_i);
                    }
                }
            }
            return false;
        }
        
        void clear(BTNode* node){
			for (int i = 0; i < node->num_children; i++) {
                clear(node->child[i]);
            }
            delete node;
        }
        
        void find_lowest_oe(BTNode* node, int min, kv_pair** min_pair, bool* found_valid){
            
			for (int i = 0; i < node->num_keys; i++) {
                //std::cout << "i: " << i << " key: " << node->keys[i] << std::endl;
                if (node->keys[i] == min) {
                    *min_pair = node->pairs[i];
                    *found_valid = true;
                    return;
                } else if (node->keys[i] > min) {
                    *min_pair = node->pairs[i];
                    *found_valid = true;
                    if (node->is_leaf == false) {
                        return find_lowest_oe(node->child[i], min, min_pair, found_valid);
                    } else {
                        return;
                    }
                }
            }

            if (node->is_leaf == false && node->num_children == node->num_keys + 1) {
                return find_lowest_oe(node->child[node->num_keys], min, min_pair, found_valid);
            }

        }
        void find_highest_ue(BTNode* node, int max, kv_pair** max_pair, bool* found_valid){

			for (int i = node->num_keys - 1; i >= 0; i--) {
                //std::cout << "i: " << i << " key: " << node->keys[i] << std::endl;
                if (node->keys[i] == max) {
                    *max_pair = node->pairs[i];
                    *found_valid = true;
                    return;
                } else if (node->keys[i] < max) {
                    *max_pair = node->pairs[i];
                    *found_valid = true;
                    if (node->is_leaf == false) {
                        if (node->num_children > i + 1) {
                            return find_highest_ue(node->child[i + 1], max, max_pair, found_valid);
                        } else {
                            return;
                        }
                    } else {
                        return;
                    }
                }
            }

            if (node->is_leaf == false && node->num_children > 0) {
                return find_highest_ue(node->child[0], max, max_pair, found_valid);
            }
        }
	public:


        bool scan(int min, int max, kv_pair** min_pair, kv_pair** max_pair){

            bool found_valid = false;
            find_lowest_oe(root, min, min_pair, &found_valid);
            if (found_valid == false) {
                std::cout << "fail1 "<< std::endl;
                return false;
            }

            found_valid = false;
            find_highest_ue(root, max, max_pair, &found_valid);
            if (found_valid == false) {
                std::cout << "fail2 "<< std::endl;
                return false;
            }
            
            std::cout << "Between: " << min << " and " << max << std::endl;

            return true;
        }

        bool get(int key, int* val){
            BTNode* result_node = nullptr;
            int index_found;
            bool found = search_step(root, key, &result_node, &index_found);
            if (found) {
                kv_pair* result_pair = result_node->pairs[index_found];
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
                        //node->num_children++;
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
        


        void insert_all(kv_pair* pairs, int len){
            for (int i = 0; i < len; i++) {
                insert(&(pairs[i]));
            }
        }

        void clear_all(){
            clear(root);
        }


        BTNode* build_step(kv_pair* pairs, int len, int depth, int height, BTNode* parent){

            BTNode* node = new BTNode();
            if (depth == height) {
                node->is_leaf = true;
            } else {
                node->is_leaf = false;
            }

            int b = max_keys;
            int m = b + 1;

            int sub_tree_size = 0;
            int child_sub_tree_size = 0;
            for (int i = 0; i < height - depth; i++) {
                child_sub_tree_size += b * pow(m, i);
            }
            int gap = child_sub_tree_size + 1;
            // std::cout << "Interval: " << gap << std::endl;
            //std::cout << "Max Keys: " << max_keys << std::endl;
            //std::cout << "Height Target: " << height << std::endl;
            // std::cout << "Length: " << len << std::endl;
            sub_tree_size = child_sub_tree_size + b * pow(m, height - depth);


            int curr_offset = 0;
            for (int i = 0; i < max_keys + 1; i++) {
                // std::cout << "loop: " << i << std::endl;

                if (curr_offset >= len) {
                    //std::cout << "break: " << depth << std::endl;
                    //break;
                } else {
                    if (i < max_keys) {
                        node->pairs[i] = pairs + curr_offset;
                        node->keys[i] = (pairs + curr_offset)->key; 
                        node->num_keys++;
                        // std::cout << "i: " << i << std::endl;
                        // std::cout << "KEY: " << (pairs + curr_offset)->key << std::endl;
                    }
                }

                
                if (curr_offset + 1 - gap < len) {
                    if (depth != height && (i != 0 || parent != nullptr)) {
                        // std::cout << "building child w/ index: " << i << std::endl;

                        BTNode* new_node = build_step(pairs + curr_offset + 1 - gap, len - (curr_offset - gap) - 1 , depth + 1, height, node);
                        node->child[i] = new_node;
                        node->num_children++;
                    }
                }

                curr_offset += gap;
            }

            //std::cout << "children loop end: " << std::endl;
            
            if (depth == 0) {
                this->root = node;
            } else if (parent == nullptr) {
                std::cout << "building parent with len: " << len - sub_tree_size << std::endl;
                node->parent = build_step(pairs + sub_tree_size, len - sub_tree_size, depth - 1, height, nullptr);
                node->parent->child[0] = node;
                node->parent->num_children++;
            } else {
                //std::cout << "depth: " << depth << std::endl;
                //std::cout << "building  rchild: " << std::endl;
                node->parent = parent;
            }

            //std::cout << "NUM CHILDREN: " << node->num_children << " NUM KEYS: " << node->num_keys << std::endl;
            //std::cout << "done: " << std::endl;
            return node;
        }

        void build(kv_pair* pairs, int len){
            int b = max_keys;
            int m = b + 1;
            int target_height = ceil( log2( static_cast<double>(len)/static_cast<double>(b) ) / log2(m) );

            build_step(pairs, len, target_height, target_height, nullptr);

            std::cout << "btree built successfully. " << std::endl;
        }
};

