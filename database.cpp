#include <iostream>
#include <algorithm>
#include <fstream> 
#include <sstream>
#include <string>
#include <iostream>

#include "memtab.cpp"
#include "btree.cpp"

class Database {
    std::string database_name;

    int memtable_size;
    memtab* memtable;

    int num_files;

    private:
        std::string get_file_by_ind(int i){
            return std::to_string(i) + (std::string)(".sst");
        }

        std::string get_next_file_name(){
            return std::to_string(num_files) + (std::string)(".sst");
        }

        std::string get_db_file_name(){
            return database_name + (std::string)(".db");
        }
        

        list_node* get_list_end(list_node* start){
            int len = start->length;
            list_node* curr = start;

            for (int i = 1; i < len; i++) {
                curr = curr->next;
            }

            return curr;
        }

        void free_linked_list(list_node* root){
            list_node* curr = root;
            while (curr != nullptr) {
                list_node* temp = curr->next;
                delete curr;
                curr = temp;
            }
        }


        kv_pair* binary_search(kv_pair** a, int len, int key){
            int pivot = (len / 2);
            if (a[pivot]->key == key) {
                return *(a + pivot);
            }

            if (len == 2){ 
                if (a[0]->key == key) {
                    return *a;
                } else if (a[1]->key == key) {
                    return *(a + 1);
                } else {
                    return nullptr;
                }
            } else if (len == 1) {
                if (a[0]->key == key) {
                    return *a;
                } else {
                    return nullptr;
                }
            }

            kv_pair* left = binary_search(a, pivot, key);
            if (left != nullptr) {
                return left;
            }
            kv_pair* right = binary_search(a + pivot + 1, len - pivot, key);
            return right;
        }

        
        btree* make_btree(kv_pair* array, int len){
            btree* t = new btree();

            for (int i = 0; i < len; i++)
            {
                t->insert(&array[i]);
            }

            return t;
        }

        void cleanup_sst_btree(btree* tree, kv_pair* array){
            tree->clear_all();
            free(array);
        }

        kv_pair* load_sst_as_list(const char* filename, int* len){
            using namespace std;
            
            std::ifstream f(filename);
            if(!f.is_open())
            {
                std:cout << "Failed to open file: " << filename << std::endl;
                return nullptr;
            }
            std::string key, val;

            int num_lines = std::count(std::istreambuf_iterator<char>(f), 
                std::istreambuf_iterator<char>(), '\n');

            std::cout << "Begin build "<< filename << std::endl;

            kv_pair* pairs = (kv_pair*)( malloc(sizeof(kv_pair) * num_lines) );

            int i = 0;
            while(std::getline(f, key, ','))
            {
                std::getline(f, val);
                
                pairs[i].key = stoi(key);
                pairs[i].value = stoi(val);
                
                i++;
            }

            *len = i;
            return pairs;
        }

    
    public: 

		Database(int cap)
		{
            database_name = "";
			memtable_size = cap;
            memtable = nullptr;
            num_files = 0;
		}

        int get(int key, int* result) {
            memtab* curr = memtable;
            int success = false;
            int curr_ind = 0;
            while (success == false) {

                success = curr->get(key, result);
                if (curr_ind != 0) {
                    //clear any temp trees
                    curr->deleteAll();
                    delete curr;
                }
                if (success == true) {
                    return success;
                }

                if (curr_ind >= num_files) {
                    return success;
                }
	            std::cout << "INT " << curr_ind << " NAME: " << get_file_by_ind(curr_ind) << std::endl;
                
                curr = build_from_file(get_file_by_ind(curr_ind).c_str());
                curr_ind++;
            }
        }

        int put(int key, int val) {
            int success = memtable->insert(key, val);
            
            if (memtable->isFull()) {
                memtable->inOrderFlush(get_next_file_name().c_str());
                num_files++;
                memtable->deleteAll();
            }

            return success;
        }

        void scan(int min, int max, kv_pair** result, int* result_length) {
            memtab* curr = memtable;
            //TODO: init linked list

            list_node* total_result;

            int curr_ind = 0;
            if (curr_ind == 0) {
                list_node* sub_result;
                //TODO: scan 'curr' and add stuff to linked list
                curr->scan(min, max, &sub_result);
                if (curr_ind != 0) {
                    //clear any temp trees
                    curr->deleteAll();
                    delete curr;
                }

                if (sub_result != nullptr && sub_result->length > 0){
                    if (total_result == nullptr) {
                        total_result = sub_result;
                    } else {
                        list_node* end = get_list_end(total_result);
                        end->next = sub_result;
                        total_result->length = total_result->length + sub_result->length;
                    }
                }

                //if (curr_ind >= num_files) {
                //    break;
                //}
                //curr = build_from_file(get_file_by_ind(curr_ind).c_str());
                curr_ind++;
            }

            for (int i = 0; i < num_files; i++) {
                int num_pairs;
                kv_pair* pairs = load_sst_as_list(get_file_by_ind(curr_ind).c_str(), &num_pairs);

                btree* tree = new btree();
                tree->treeify(pairs, num_pairs);

                //kv_pair* min_pair = binary_search(&pairs, num_pairs, min);
                //kv_pair* max_pair = binary_search(&pairs, num_pairs, max);
                kv_pair* min_pair; kv_pair* max_pair;
                bool success = tree->scan(min, max, &min_pair, &max_pair);
                kv_pair* curr = min_pair;

                if (success == true) {
                    while (curr != max_pair + 1) {
                        kv_pair* next = new kv_pair(curr->key, curr->value);
                        result[result_length] = 

                        curr++;
                    }
                }         

                tree->clear_all();       
                
            }
            
            *result = (kv_pair*)malloc(sizeof(kv_pair) * total_result->length);
            list_node* currResult = total_result;
            
            for (int i = 0; i < total_result->length; i++) {
                (*result)[i].key = currResult->key;
                (*result)[i].value = currResult->value;
                currResult = currResult->next;
            }
            free_linked_list(total_result);
        }



        int open(const char* database_name) {
            this->database_name = database_name;

 

            //check if database alr exists
            std::ifstream f(get_db_file_name());
	        if(!f.is_open()) {
                //must make new db
                num_files = 0;
                memtable = new memtab(memtable_size);
                
            } else {
                //load the db
                std::string temp;
                std::getline(f, temp);

                num_files = stoi(temp);
                
                std::getline(f, temp);
                int buffer_empty = stoi(temp);

                if (buffer_empty == 1) { 
                    memtable = new memtab(memtable_size);
                } else {
                    memtable = build_from_file(get_file_by_ind(num_files).c_str());
                }
            }

            //TODO: report errors
            return 0;
        }

        void close() {

            int buffer_empty = 1;
            if (memtable->getEntries() >= 0){
	            std::cout << "size "<< memtable->getEntries() << std::endl;
                memtable->inOrderFlush(get_next_file_name().c_str());
                buffer_empty = 0;
            }

            std::ofstream output(get_db_file_name(), std::ofstream::trunc);

            output << num_files << "\n" << buffer_empty << "\n" ;

            output.close();

            delete memtable;
        }
};




//for testing db
int main() {

    Database* db = new Database(3);

	db->open("testDB");

    db->put(2, 7);
    db->put(3, 8);
    db->put(4, 9);
    db->put(5, 10);

    int result;
    db->get(2, &result);
    std::cout << "result: " << result << std::endl;

	db->close();

	db->open("testDB");
    db->put(1, 2);
	db->close();
	
	
    return 0;
}