#include <iostream>
#include <algorithm>
#include <fstream> 
#include <sstream>
#include <string>
#include <iostream>
#include "btree.cpp"
#include <random>
#include <time.h>  
#include <cmath>


struct bucket_node
{//chaining the buckets
	bucket_node * next;
	memtab * table;	
	int entries;
	bucket_node(): next(nullptr), table(nullptr), entries(0){}
};

static unsigned char bitHash(int bits, unsigned char key)
{//we hash the key by choosing some number of leading bits
	unsigned char mask= 0;
	for(int i = 0; i < 5; i++) // subroutine to make bit mask
	{
		unsigned char temp = mask << 1;
		mask = temp | 1;
	}
	return (unsigned char)(mask & key);
}


class Database {
    std::string database_name;
	
	//stuff for the buffer
	const int min_buffer_depth = 3;//allows 2^3 entries, initializes this size
	const int max_buff_depth = 5;//no more than 32 entries.
	int curr_buffer_depth;
	int curr_buffer_entries;
	int policy; //NOTE TO SELF######: assign policies to ints. (0 is lru, 1 is clock, etc)
	
    int memtable_size;
    memtab* memtable;
	bucket_node ** buffer_directory;
	
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
//----------------------------------buffer methods begin-----------------------------------------------
		void initializeBuffer()
		{
			buffer_directory = (bucket_node**)(malloc(pow(2,min_buffer_depth) * sizeof(bucket_node*)));
			curr_buffer_depth = min_buffer_depth;
			for(int i = 0; i < pow(2, curr_buffer_depth); i++)
			{//assign memory for the initial buffer buckets
				buffer_directory[i] = new bucket_node();
			}
			curr_buffer_entries = 0;
		}
		
		void insertIntoBuffer(memtab* table)
		{//inserts table into buffer, evicts if needed
		
			curr_buffer_entries++;//check for space in buffer, evict if needed 
			if(curr_buffer_entries > pow(2, curr_buffer_depth))
			{
				if(curr_buffer_depth >= max_buff_depth)
					evict();
				else
					doubleBufferSize();
			}
		
			unsigned char hash = bitHash(curr_buffer_depth, table->key);
			
			bucket_node* curr_head = buffer_directory[int(hash)];
			while(curr_head->next)
				curr_head = curr_head->next;
						
			if(!curr_head->table)
			{
				curr_head->table = (memtab*)malloc(sizeof(memtab));//save this table in heap, referenced to by this bucket node
				std::memcpy(curr_head->table, table, sizeof(memtab));
				
				Node* newRoot = (Node*)(malloc(sizeof(Node)));//in addition to keeping the table in memory, have to traverse the tree and do the same.
				std::memcpy(newRoot, table->root, sizeof(Node));
				curr_head->table->root = newRoot;
				heapifyTree(curr_head->table->root);
				
				curr_head->next = new bucket_node();
			}			
		}
		void evict()
		{//policy dependant
		}
		void evict_bucket(bucket_node * n)
		{//evict a certain memtab
		
			freeTree(n->table->root);
			free(n->table); 
		}
		void doubleBufferSize()
		{
			curr_buffer_depth++;
			if(curr_buffer_depth >= max_buff_depth)
			{
				curr_buffer_depth--;
				std::cerr << "Warning, can't double buffer size" << std::endl;
				return;
			}
			
			bucket_node ** temp = (bucket_node**)(malloc(pow(2, curr_buffer_depth-1) * sizeof(bucket_node)));
			temp = buffer_directory;
			//new directory with double the size.
			buffer_directory = (bucket_node**)(malloc(pow(2,min_buffer_depth) * sizeof(bucket_node*)));
			for(int i = 0; i < pow(2, curr_buffer_depth); i++)
			{	
				buffer_directory[i] = new bucket_node();
			}
			//re insert previous directory into the new one.
			for(int i = 0; i < pow(2, curr_buffer_depth-1); i++)
			{	
				bucket_node* curr = temp[i];
				if(curr->table)
				{
					insertIntoBuffer(curr->table);
					bucket_node* tempnext = curr->next;
					evict_bucket(curr);
					curr = tempnext;
				}
			}
			free(temp);
		}
		
//----------------------------------buffer methods end-------------------------------------------------	


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
            f.clear();
            f.seekg(0);

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
			srand(time(NULL));
			initializeBuffer();
		}

        int get(int key, int* result) {
            memtab* curr = memtable;
            int success = false;
            int curr_ind = 0;
            if (curr_ind == 0) {

                success = curr->get(key, result);
                if (curr_ind != 0) {
                    //clear any temp trees
                    curr->deleteAll();
                    delete curr;
                }
                if (success == true) {
                    return success;
                }

                //if (curr_ind >= num_files) {
                //    return success;
                //}
	            //std::cout << "INT " << curr_ind << " NAME: " << get_file_by_ind(curr_ind) << std::endl;
                
                //curr = build_from_file(get_file_by_ind(curr_ind).c_str());
                curr_ind++;
            }

            

            for (int i = 0; i < num_files; i++) {
                int num_pairs;
                kv_pair* pairs = load_sst_as_list(get_file_by_ind(i).c_str(), &num_pairs);

                btree* tree = new btree();
                //tree->insert_all(pairs, num_pairs);
                tree->build(pairs, num_pairs);

                bool success = tree->get(key, result);

                if (success == true) {
                    return success;
                }         

                tree->clear_all();    
            }

            return false;
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

            list_node* total_result = nullptr;


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
                kv_pair* pairs = load_sst_as_list(get_file_by_ind(i).c_str(), &num_pairs);

                btree* tree = new btree();
                tree->build(pairs, num_pairs);

                //kv_pair* min_pair = binary_search(&pairs, num_pairs, min);
                //kv_pair* max_pair = binary_search(&pairs, num_pairs, max);
                kv_pair* min_pair; kv_pair* max_pair;
                bool success = tree->scan(min, max, &min_pair, &max_pair);
                kv_pair* curr_pair = min_pair;

                if (success == true) {
                    list_node* sub_result;
                    list_node* sub_curr;
                    while (curr_pair != max_pair + 1) {
                        list_node* sub_new = new list_node(curr_pair->key, curr_pair->value);
                        if (sub_result == nullptr) {
                            sub_result = sub_new;
                        } else {
                            sub_curr->next = sub_new;
                        }
                        sub_curr = sub_new;

                        curr_pair++;
                    }

                    //connect results
                    if (total_result == nullptr) {
                        total_result = sub_result;
                    } else {
                        list_node* end = get_list_end(total_result);
                        end->next = sub_result;
                        total_result->length = total_result->length + sub_result->length;
                    }
                }         

                
                tree->clear_all();
                
            }
            
            
            *result = (kv_pair*)malloc(sizeof(kv_pair) * total_result->length);
            list_node* currResult = total_result;
            
            *result_length = total_result->length;
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
	            //std::cout << "size "<< memtable->getEntries() << std::endl;
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
    db->put(6, 10);

    int result;
    db->get(2, &result);
    std::cout << "result 1: " << result << std::endl;

    kv_pair* results; 
    int results_len = 0; 
    db->scan(5, 1000, &results, &results_len);
    std::cout << "result 2: " << results->key << " w/ len " << results_len << std::endl;

	db->close();

	db->open("testDB");
    db->put(1, 2);
	db->close();
	
	
    return 0;
}