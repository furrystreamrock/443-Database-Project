#include <algorithm>
#include <fstream> 
#include <sstream>
#include <string>
#include <iostream>
#include "btree.cpp"
#include <random>
#include <time.h>  
#include <cmath>



static unsigned long bitHash(int bits, unsigned long key)
{//we hash the key by choosing some number of leading bits
	unsigned long mask= 0;
	for(int i = 0; i < bits; i++) // subroutine to make bit mask
	{
		unsigned long temp = mask << 1;
		mask = temp | 1;
	}
	return (unsigned long)(mask & key);
}

static const int SST_BYTES = 4096 - sizeof(SST) - 5; //just shy of 4kb per sst including metadata
static const int MAX_ENTRIES = SST_BYTES / sizeof(kv_pair);

class Database {
    std::string database_name;
	
	//stuff for the buffer
	const int min_buffer_depth = 2;
	const int max_buff_depth = 5;
	int curr_buffer_depth;
	int curr_buffer_entries;
	int evict_policy; //NOTE TO SELF######: assign policies to ints. (0 is lru, 1 is clock, etc)
	node_dll* lru_head;
	node_dll* lru_tail;
	
	
    int memtable_size;
    memtab* memtable;
	bucket_node ** buffer_directory;
	
    int num_files;

    lsmtree* db_lsmt;

    private:
        void increment_num_files(){
            if (STAGE == 3) {

            } else {
                num_files++;
            }
        }

        int get_num_files(){
            if (STAGE == 3) {
                return db_lsmt->get_num_pages();
            } else {
                return num_files;
            }
        }

        void set_num_files(int val){
            if (STAGE == 3) {

            } else {
                num_files = val;
            }
        }

        std::string get_file_by_ind(int i){
            return std::to_string(i) + (std::string)(".sst");
        }

        std::string get_next_file_name(){
            return std::to_string(get_num_files()) + (std::string)(".sst");
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
			evict_policy = 0;
			lru_head = nullptr;
			lru_tail = nullptr;
		}
		
		void insertIntoBuffer(SST* sst, bool reinsert = false)
		{//inserts table into buffer, evicts if needed
		//should be done on pages we know aren't already in the buffer
			
				
			if(curr_buffer_entries >= pow(2, curr_buffer_depth) * 1)//85% capacity threshold
			{
				if(curr_buffer_depth >= max_buff_depth)
					evict();
				else
					doubleBufferSize();
			}
			curr_buffer_entries++;
			unsigned long hash = bitHash(curr_buffer_depth, sst->key);
			bucket_node* curr_head = buffer_directory[hash];
			//std::cout << curr_buffer_depth << std::endl;
			std::cout << "Buffer pages: " << curr_buffer_entries << std::endl;
			std::cout << "Inserting SST: " << sst->key << " at hash: "<<  int(hash) << std::endl;
			if(!curr_head)
				return;
			while(curr_head->next)
				curr_head = curr_head->next;
			
			curr_head->sst = (SST*)malloc(sizeof(SST));
			std::memcpy(curr_head->sst, sst, sizeof(SST));
			
			curr_head->sst->data = (kv_pair*)(malloc(SST_BYTES*sizeof(kv_pair)));
			std::cout << sst->data[0].key << std::endl;
			std::memcpy(curr_head->sst->data, sst->data, SST_BYTES*sizeof(kv_pair));
			
			curr_head->next = new bucket_node();
			curr_head->next->prev = curr_head;
			

			//LRU
			if(evict_policy == 0)
				lruUpdate(curr_head);
			node_dll* a = lru_head;//show LRU queue for debug
			while(a)
			{
				std::cout << a->target->sst->key << "|";
				//std::cout << bitHash(curr_buffer_depth, a->target->sst->key) << "|";
				a = a->next;
			}
			std::cout << std::endl;
			printBuckets();
			
		}	
		void evict()
		{//policy dependant
			if(evict_policy == 0)
				lruEvict();
			if(evict_policy == 1)
				clockEvict();
			
			curr_buffer_entries--;
		}
		
		void lruEvict()
		{//remove the tail of lru, should free all associated memory in that page as well
			std::cout << "check 1" << std::endl;
			if(!lru_tail)
				std::cerr << "Warning! tried to evict in empty buffer." << std::endl;
			
			if(!lru_tail->target->prev)//first in the bucket.
			{
				buffer_directory[bitHash(curr_buffer_depth, lru_tail->target->sst->key)] = lru_tail->target->next;
				lru_tail->target->next->prev = nullptr;
			}
			else
			{
				lru_tail->target->prev->next = lru_tail->target->next;
				lru_tail->target->next->prev = lru_tail->target->prev;
			}
			std::cout << "check 2" << std::endl;
			cleanBucket(lru_tail->target);
			delete(lru_tail->target);
			node_dll* temp = lru_tail->prev;
			std::cout << "Evicting " << lru_tail->target->sst->key << " in bucket: " << bitHash(curr_buffer_depth, lru_tail->target->sst->key) << std::endl;
/* 			if(lru_tail->target->prev && lru_tail->target->prev->sst)
				std::cout << "Prev: " << lru_tail->target->prev->sst->key << std::endl;
			if(lru_tail->target->next && lru_tail->target->next->sst)
				std::cout << "Next: " << lru_tail->target->next->sst->key << std::endl;
 */
			delete(lru_tail);
			lru_tail = temp;
			if(lru_tail)
				lru_tail->next = nullptr;
			std::cout << "check 3" << std::endl;
		}
		void lruUpdate(bucket_node* target)
		{//new get or insert updates a page
			node_dll* new_lru = new node_dll();
			new_lru->next = lru_head;
			if(new_lru->next)
				new_lru->next->prev = new_lru;
			new_lru->target = target;
			lru_head = new_lru;
			if(!lru_tail)
				lru_tail = new_lru;
		}
		void clockEvict()
		{}
		void cleanBucket(bucket_node * n)
		{//evict a certain memtab
			free(n->sst->data);
			free(n->sst);
		}
		
		void doubleBufferSize()
		{
			std::cout << "Doubling buffer capacity, new depth: " << curr_buffer_depth + 1 << std::endl;
			if(curr_buffer_depth >= max_buff_depth)
			{
				curr_buffer_depth--;
				std::cerr << "Warning, can't double buffer size" << std::endl;
				return;
			}
			curr_buffer_depth++;
						
			//new directory with double the size.
			bucket_node** new_buffer_directory = (bucket_node**)(malloc(pow(2,curr_buffer_depth) * sizeof(bucket_node*)));
			for(int i = 0; i < pow(2, curr_buffer_depth); i++)
			{	
				new_buffer_directory[i] = new bucket_node();
			}
			//re insert previous directory into the new one.
			
			for(int i = 0; i < pow(2, curr_buffer_depth-1); i++)
			{	
				bucket_node* curr = buffer_directory[i];
				while(curr)
				{
					bucket_node* temp = (bucket_node*)curr->next;
					if(!curr)//somehow segfaulting without this line
						break;
					if(curr->sst)
					{
						std::cout << "Re-inserting SST: " << curr->sst->key << std::endl;
						reinsertBucket(curr, new_buffer_directory);
					}
					curr = temp;
				}
			}
			free(buffer_directory);
			buffer_directory = new_buffer_directory;
		}
		
		void reinsertBucket(bucket_node* b, bucket_node** dir)
		{//
			b->next = nullptr;
			b->prev = nullptr;
			bucket_node* head = dir[bitHash(curr_buffer_depth, b->sst->key)];
			if(!head->next)
			{
				 dir[bitHash(curr_buffer_depth, b->sst->key)] = b;
				 b->next = new bucket_node();
				 b->next->prev = b;
			}
			else
			{
				while(head->next)
					head = head->next;
				
				head->prev->next = b;
				b->next = head;
				b->prev = head->prev;
				head->prev = b;
			}
			if(!b->next)
			{
				b->next = new bucket_node();
				b->next->prev = b;
			}
		}
		
		void printBuckets()
		{
			std::cout << "------------------------------------------" << std::endl;
			for(int i = 0; i < pow(2, curr_buffer_depth); i++)
			{	
				bucket_node* curr = buffer_directory[i];
				std::cout << i << "  ";
				while(curr)
				{
					if(curr->sst && curr->sst->key)
						std::cout <<  curr->sst->key << "-";
					else
						std::cout <<  "*" << "-";
					curr = curr->next;
				}
				std::cout << std::endl;
			}
			std::cout << "------------------------------------------" << std::endl;
		}
		
		SST* getSST(unsigned long key)
		{//returns the table pointer to table in buffer on success, nullptr on failure
			
			bucket_node* head = buffer_directory[int(bitHash(curr_buffer_depth, key))];
			while(head && head->sst && head->next)
			{
				
				if(head->sst->key == key)
					return head->sst;
				head = head->next;
			}
			return nullptr;
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

                std::cout << "curr: " << curr->key << std::endl;
                list_node* temp = curr->next;
                delete curr;
                curr = temp;
            }
        }


        bool binary_search(kv_pair* a, int len, int key, kv_pair** result){
            int pivot = (len / 2);
            if (a[pivot].key == key) {
                *result = (a + pivot);
                return true;
            }

            if (len == 2){ 
                if (a[0].key == key) {
                    *result = a;
                    return true;
                } else if (a[1].key == key) {
                    *result = (a + 1);
                    return true;
                } else {
                    if (abs(a[0].key - key) > abs(a[1].key - key)) {
                        *result = (a + 1);
                    } else {
                        *result = (a);
                    }
                    return false;
                }
            } else if (len == 1) {
                *result = a;
                if (a[0].key == key) {
                    return true;
                } else {
                    return false;
                }
            }

            if (a[pivot].key > key) {
                return binary_search(a, pivot, key, result);
            } else {
                return binary_search(a + pivot + 1, len - pivot, key, result);
            }
            return false;
        }
        bool binary_scan(kv_pair* a, int len, int min, int max, kv_pair** start, kv_pair** end){
            kv_pair* result_min = nullptr;
            bool found_min = binary_search(a, len, min, &result_min);
            if (!found_min) {
                if (result_min->key < min){
                    if (result_min - a == 0){
                        return false;
                    }
                    result_min += 1;
                }
            }
            *start = result_min;

            
            kv_pair* result_max = nullptr;
            bool found_max = binary_search(a, len, max, &result_max);
            if (!found_max) {
                if (result_max->key > max){
                    if (result_max - a == len){
                        return false;
                    }
                    result_max -= 1;
                }
            }
            *end = result_max;

            return true;
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
                std::cout << "Failed to open file: " << filename << std::endl;
                return nullptr;
            }
            std::string key, val;

            int num_lines = std::count(std::istreambuf_iterator<char>(f), 
                std::istreambuf_iterator<char>(), '\n');

            //std::cout << "Building sst: "<< filename << ", size " << num_lines << std::endl;

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

		Database()
		{
            database_name = "";
			memtable_size = PAGE_LENGTH;
            memtable = nullptr;
            num_files = 0;
			srand(time(NULL));
			initializeBuffer();

            db_lsmt = nullptr;
		}

        int get(int key, int* result) {
            memtab* buf = memtable;
            int success = false;
            if (buf->getEntries() > 0) {

                success = buf->get(key, result);
                if (success == true) {
                    return success;
                }
            }

            

            for (int i = get_num_files() - 1; i >= 0; i--) {
                int num_pairs = 0;
                kv_pair* pairs = nullptr;
                
                if (STAGE == 3) {
                    if (db_lsmt->check_filter(i, key) == true) {
                        db_lsmt->read_page(i, &pairs, &num_pairs);
                    }  else {
                        continue;
                    }
                } else {
                    pairs = load_sst_as_list(get_file_by_ind(i).c_str(), &num_pairs);
                }

                
                if (STAGE == 1) {
                    kv_pair* sst_result;
                    bool found = binary_search(pairs, num_pairs, key, &sst_result)  ;
                    if (found == true) {
                        *result = sst_result->value;
                        //FREES THE LOADED PAIRLIST
                        free(pairs);
                        return found;
                    }

                } else if (STAGE >= 2) {
                    btree* tree = new btree();
                    //tree->insert_all(pairs, num_pairs);
                    tree->build(pairs, num_pairs);

                    // for (int i = 0; i< num_pairs; i++){
				    //     std::cout<< "2key: " << pairs[i].key << std::endl;
                    // }

                    bool success = tree->get(key, result);

                    if (success == true) {
                        free(pairs);
                        return success;
                    }

                    tree->clear_all();    
                }

                //FREES THE LOADED PAIRLIST
                free(pairs);
            }

            return false;
        }
		void TESTINGBUFFER()
		{//stress test buffer
			/*IMPORTANT NOTE****** im not sure if its my system, but if I run at 10 pages for 10 trials,
			I will get a segfault ~10% of the time.
			However if I run 1000 pages 10 times, I will still get a 10% segfault rate.
			Unsure if its instability with using a mirror compiler
			*/
			srand(time(NULL));
			unsigned int test_key = 0;
			for(int i = 0; i < 300; i++)
			{
				SST* tab0 = new SST();
				std::cout<< "Created table :" << tab0->key << std::endl;
				tab0->data = (kv_pair*)(malloc(SST_BYTES * sizeof(kv_pair)));
				for(int j = 0; j < 10; j++)
				{
					tab0->data[j].key = i*100+j;
					tab0->data[j].value = i*100+j;
				}
				std::cout << "----------------------INSERTING: " << i << "----------------------" << std::endl;		
				insertIntoBuffer(tab0);
				free(tab0->data);
				
				if(i == 297)
					test_key = tab0->key;
				
				delete(tab0);
				
			}
			
			std::cout << "Finished insertions" << std::endl;

			SST* getResult =  getSST(test_key);
			
			if(!getResult)
			{
				std::cout << "test_key SST was evicted" << std::endl;
				return;
			}
			std::cout << "Testing to retrieve table: " << getResult->key << std::endl;
			for(int i = 0; i < 10; i++)
			{
				std::cout << getResult->data[i].value << std::endl;
			}
			
			
		}
        int put(int key, int val) {
            int success = memtable->insert(key, val);
            
            if (memtable->isFull()) {
                memtable->inOrderFlush(get_next_file_name().c_str(), db_lsmt);
                increment_num_files();
                memtable->deleteAll();
            }

            return success;
        }

        int update(int key, int val) {
            //TODO figure out if we have to make sure key already exists; or maybe we dont need a seperate method for updating
            return put(key, val);
        }
        
        int del(int key) {
            //delete
            return put(key, TOMBSTONE);
        }

        void scan(int min, int max, kv_pair** result, int* result_length) {
            memtab* buf = memtable;

            //std::cout << "Scan memtab: " << std::endl;

            kv_pair* result_temp = nullptr;
            int result_temp_len = 0;

            if (buf->getEntries() > 0) {
                list_node* sub_result = nullptr;
                //TODO: scan 'curr' and add stuff to linked list
                buf->scan(min, max, &sub_result);

                if (sub_result != nullptr && sub_result->length > 0){
                    result_temp = (kv_pair*)malloc(sizeof(kv_pair) * sub_result->length);
                    for (int i = 0; i < sub_result->length; i++) {
                        result_temp[i].key = sub_result->key;
                        result_temp[i].value = sub_result->value;
                        sub_result = sub_result->next;
                    }
                    result_temp_len = sub_result->length;
                }

            }

            //std::cout << "Scan sst begin: " << std::endl;
            for (int i = get_num_files() - 1; i >= 0; i--) {
                int num_pairs = 0;
                kv_pair* pairs = nullptr;

                if (STAGE == 3) {
                    db_lsmt->read_page(i, &pairs, &num_pairs);
                } else {
                    pairs = load_sst_as_list(get_file_by_ind(i).c_str(), &num_pairs);
                }
                bool success = false;

                kv_pair* min_pair = nullptr; 
                kv_pair* max_pair = nullptr;
                if (STAGE == 1) {
                    success = binary_scan(pairs, num_pairs, min, max, &min_pair, &max_pair);


                } else if (STAGE >= 2) {

                    btree* tree = new btree();
                    tree->build(pairs, num_pairs);

                    success = tree->scan(min, max, &min_pair, &max_pair);

                    
                    tree->clear_all();
                } else {

                }

                if (success == true && min_pair != nullptr && max_pair != nullptr) {
                    //combine
                    int sub_result_len = 0;
                    kv_pair* temp_pair = min_pair;
                    while (temp_pair != max_pair + 1) {
                        sub_result_len++;
                        temp_pair++;
                    }

                    if (result_temp == nullptr) {
                        result_temp = min_pair;
                        result_temp_len = sub_result_len;
                    } else {
                        int merged_len = 0;
                        kv_pair* merged_result = lsmtree::merge_list(result_temp, result_temp_len, min_pair, sub_result_len, &merged_len);
                        free(result_temp);
                        result_temp = merged_result;
                        result_temp_len = merged_len;
                    }
                }

            }

            *result = result_temp;
            *result_length = result_temp_len;
            return;

        }




        void scan_old(int min, int max, kv_pair** result, int* result_length) {
            memtab* curr = memtable;
            //TODO: init linked list

            list_node* total_result = nullptr;

            std::cout << "Scan memtree: " << std::endl;
            int curr_ind = 0;
            if (curr_ind == 0) {
                list_node* sub_result;
                //TODO: scan 'curr' and add stuff to linked list
                curr->scan(min, max, &sub_result);

                if (sub_result != nullptr && sub_result->length > 0){
                    total_result = sub_result;
                    
                    for (int j = 0; j < sub_result->length; j++) {
                        std::cout << sub_result[j].key << ";";
                    }
                    std::cout << std::endl;
                }

                curr_ind++;
            }

            std::cout << "Scan sst begin: " << std::endl;

            for (int i = 0; i < get_num_files(); i++) {
                int num_pairs;
                kv_pair* pairs = load_sst_as_list(get_file_by_ind(i).c_str(), &num_pairs);
                bool success = false;

                kv_pair* min_pair = nullptr; 
                kv_pair* max_pair = nullptr;
                if (STAGE == 1) {
                    //std::cout << "SCAN " << i << std::endl;
                    success = binary_scan(pairs, num_pairs, min, max, &min_pair, &max_pair);
                    //std::cout << "SCAN2 " << i << std::endl;


                } else if (STAGE == 2) {

                    btree* tree = new btree();
                    tree->build(pairs, num_pairs);

                    //kv_pair* min_pair = binary_search(&pairs, num_pairs, min);
                    //kv_pair* max_pair = binary_search(&pairs, num_pairs, max);
                    success = tree->scan(min, max, &min_pair, &max_pair);

                    
                    tree->clear_all();
                } else {

                }


                if (success == true) {
                    kv_pair* curr_pair = min_pair;
                    
                    bool found = false;
                    if (curr_pair != max_pair + 1){
                        found = true;
                        std::cout << "Scan sst " << i << ":" << std::endl;
                    }

                    int sub_len = 0;
                    list_node* sub_result = nullptr;
                    list_node* sub_curr = nullptr;
                    while (curr_pair != max_pair + 1) {
                        list_node* sub_new = new list_node(curr_pair->key, curr_pair->value);
                        if (sub_result == nullptr) {
                            sub_result = sub_new;
                        } else {
                            sub_curr->next = sub_new;
                        }
                        sub_curr = sub_new;

                        
                        std::cout << curr_pair->key << ";";

                        curr_pair++;
                        sub_len++;
                    }
                    if (found){
                        std::cout << std::endl;
                    }
                    if (sub_len > 0) {
                        sub_result->length = sub_len;
	                    //std::cout << "sub result length "<< sub_result->length << std::endl;
                    }

                    //connect results

                    //TODO: merge and take out duplicates
                    if (sub_result != nullptr) {
                        if (total_result == nullptr) {
                            total_result = sub_result;
                            //std::cout << "new total result length "<< total_result->length << std::endl;
                        } else { }
                    }

                    if (sub_result != nullptr) {
                        if (total_result == nullptr) {
                            total_result = sub_result;
                            //std::cout << "new total result length "<< total_result->length << std::endl;
                        } else {
                            int length = total_result->length;

                            list_node* curr_total = total_result;
                            list_node* curr_new = sub_result;
                            list_node* prev_total = nullptr;
                            bool increment_total = true;
                            while (curr_new != nullptr) {
                                list_node* next_new = curr_new->next;
                                if (curr_total == nullptr) {
                                    //add to end
                                    prev_total->next = curr_new;
                                    curr_total = curr_new;
                                    curr_new->next = nullptr;
                                    
                                    curr_new = next_new;
                                } else if (curr_new->key == curr_total->key) {
                                    //duplicate, keep the newer one (should alr be in the total)
                                    list_node* old_new = curr_new;
                                    curr_new = curr_new->next;
                                    old_new->next = nullptr;
                                    free_linked_list(old_new);
                                } else if (curr_new->key < curr_total->key) {
                                    //add middle or beginning
                                    curr_new->next = curr_total;
                                    if (prev_total != nullptr) {
                                        prev_total->next = curr_new;
                                    } else {
                                        total_result = curr_new;
                                    }

                                    curr_new = next_new;
                                    increment_total = false;
                                    prev_total = curr_new;
                                } else {
                                    //cant add here

                                }

                                if (increment_total == true) {
                                    prev_total = curr_total;
                                    if (curr_total != nullptr){
                                        curr_total = curr_total->next;
                                    }
                                }
                            }

                            
                            //list_node* curr = 
                            // list_node* end = get_list_end(total_result);
                            // if (sub_result != nullptr) {
                            //     int sub_len = 0;
                            //     sub_len = sub_result->length;
                            //     end->next = sub_result;
                            //     total_result->length = total_result->length + sub_len;
                            //     //std::cout << "new total result length 2 "<< total_result->length << std::endl;
                            // }
                        }
                    }
                }
                
                //TODO: someth in memtab appears to delete the last loaded sst as well
                //if (i != num_files) {
                    //FREES THE LOADED PAIRLIST
                    free(pairs);
                //}
            }
            

            int res_len = 0;       
            if (total_result != nullptr) {
                *result = (kv_pair*)malloc(sizeof(kv_pair) * total_result->length);
                list_node* currResult = total_result;
                
                *result_length = total_result->length;
                for (int i = 0; i < total_result->length; i++) {
                    std::cout << "i: " << i << " key: " << currResult->key << std::endl;
                    (*result)[i].key = currResult->key;
                    (*result)[i].value = currResult->value;
                    currResult = currResult->next;
                }
                std::cout << "result length "<< total_result->length << std::endl;

                free_linked_list(total_result);
            } else {
                std::cout << "nothing found. " << std::endl;
                *result = nullptr;
            }
        }


        void reset(const char* database_name) {
            this->database_name = database_name;

            std::ifstream f(get_db_file_name());
	        if(!f.is_open()) {
                
            } else {
                std::cout << "Deleting all. " << std::endl;
                //load the db
                std::string temp;
                std::getline(f, temp);

                set_num_files(stoi(temp));
                
                std::getline(f, temp);
                int buffer_empty = stoi(temp);

                if (buffer_empty == 0) { 
                    increment_num_files();
                }

                remove(get_db_file_name().c_str());

                this->db_lsmt = new lsmtree(database_name);
                this->db_lsmt->load_tree();

                for (int i = 0; i < get_num_files(); i++) {
                    remove(get_file_by_ind(i).c_str());
                }

                this->db_lsmt->del_tree();
                delete this->db_lsmt;
            }

            this->database_name = "";
        }

        int open(const char* database_name) {
            this->database_name = database_name;

            this->db_lsmt = new lsmtree(database_name);
            this->db_lsmt->load_tree();

            //check if database alr exists
            std::ifstream f(get_db_file_name());
	        if(!f.is_open()) {
                //must make new db
                std::cout << "Making new DB. " << std::endl;
                num_files = 0;
                memtable = new memtab(memtable_size);

            } else {
                //load the db
                std::cout << "Loading DB. " << std::endl;
                std::string temp;
                std::getline(f, temp);

                set_num_files(stoi(temp));
                
                std::getline(f, temp);
                int buffer_empty = stoi(temp);

                if (buffer_empty == 1) { 
                    memtable = new memtab(memtable_size);
                } else {
                    memtable = build_from_file(get_file_by_ind(get_num_files()).c_str());
                }
            }

            //TODO: report errors
            return 0;
        }
	
        void close() {

            int buffer_empty = 1;
            if (memtable->getEntries() > 0){
	            std::cout << "Memtab size on close: "<< memtable->getEntries() << std::endl;
                memtable->inOrderFlush(get_next_file_name().c_str(), nullptr);
                buffer_empty = 0;
            }

            std::ofstream output(get_db_file_name(), std::ofstream::trunc);

            output << get_num_files() << "\n" << buffer_empty << "\n" ;

            output.close();

            this->db_lsmt->save_tree();
            delete db_lsmt;
            delete memtable;
        }
		
		/* void printBuffer()
		{
			for(int i = 0; i < pow(2, curr_buffer_depth); i++)
			{	
				bucket_node* curr = buffer_directory[i];
				while(curr && curr->table)
				{
					curr->sst->printInorder();
					curr = curr->next;
				}
			}
		} */
};

