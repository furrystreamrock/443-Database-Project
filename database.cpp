#include <iostream>
#include <algorithm>
#include <fstream> 
#include <sstream>
#include <string>
#include <iostream>
#include "memtab.cpp"
#include <random>
#include <time.h>  
#include <cmath>


struct bucket_node
{//chaining the buckets
	bucket_node * prev;
	bucket_node * next;
	memtab * table;	
	bool clockbit;
	bucket_node(): prev(nullptr), next(nullptr), table(nullptr), clockbit(false){}
};

struct node_dll
{
	node_dll* next;
	node_dll* prev;
	bucket_node* target;
	
	node_dll(): next(nullptr), prev(nullptr), target(nullptr){}
};


static unsigned char bitHash(int bits, unsigned char key)
{//we hash the key by choosing some number of leading bits
	unsigned char mask= 0;
	for(int i = 0; i < bits; i++) // subroutine to make bit mask
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
	int evict_policy; //NOTE TO SELF######: assign policies to ints. (0 is lru, 1 is clock, etc)
	node_dll* lru_head;
	node_dll* lru_tail;
	
	
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
			evict_policy = 0;
			lru_head = nullptr;
			lru_tail = nullptr;
		}
		
		void insertIntoBuffer(memtab* table)
		{//inserts table into buffer, evicts if needed
		
			curr_buffer_entries++;//check for space in buffer, evict if needed 
			if(curr_buffer_entries > pow(2, curr_buffer_depth) * 0.85)//85% capacity threshold
			{
				if(curr_buffer_depth >= max_buff_depth)
					evict();
				else
					doubleBufferSize();
			}
			std::cout << "checkpoint 1" << std::endl;
			unsigned char hash = bitHash(curr_buffer_depth, table->key);
			bucket_node* curr_head = buffer_directory[int(hash)];
			std::cout << curr_buffer_depth << std::endl;
			std::cout << int(hash) << std::endl;
			if(curr_head)
				std::cout << "checkpoint 1.5" << std::endl;
			
			while(curr_head->next)
				curr_head = curr_head->next;
			
			std::cout << "checkpoint 2" << std::endl;
			if(!curr_head->table)
			{
				curr_head->table = (memtab*)malloc(sizeof(memtab));//save this table in heap, referenced to by this bucket node
				std::memcpy(curr_head->table, table, sizeof(memtab));
				curr_head->clockbit = true;
				Node* newRoot = (Node*)(malloc(sizeof(Node)));//in addition to keeping the table in memory, have to traverse the tree and do the same.
				std::memcpy(newRoot, table->root, sizeof(Node));
				curr_head->table->root = newRoot;
				heapifyTree(curr_head->table->root);
				std::cout << "checkpoint 3" << std::endl;
				curr_head->next = new bucket_node();
				curr_head->next->prev = curr_head;
			}	
			std::cout << "checkpoint 4" << std::endl;
		}
		void evict()
		{//policy dependant
			if(evict_policy == 0)
				lruEvict();
			if(evict_policy == 1)
				clockEvict();
		}
		
		void lruEvict()
		{
			bucket_node* toEvict = lru_tail->target;
			
			//process the bucket/directory to remove the page
			if(toEvict->prev)
				toEvict->prev->next = toEvict->next;
			evict_bucket(toEvict);
			
			//update the double linked eviction list
			lru_tail->prev->next = nullptr;//new tail
			node_dll* temp = lru_tail->prev ? lru_tail->prev : nullptr;//redundant? this is intended implemntation but mby not needed
			delete(lru_tail);
			lru_tail = temp;
			
		}
		void clockEvict()
		{}
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
			
			bucket_node ** temp = (bucket_node**)(malloc(pow(2, curr_buffer_depth-1) * sizeof(bucket_node*))); //hold the pointers to the buckets in temp.
			std::memcpy(temp, buffer_directory, pow(2,curr_buffer_depth-1) * sizeof(bucket_node*));
			
			free(buffer_directory);
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
				while(curr && curr->table)
				{
					insertIntoBuffer(curr->table);
					bucket_node* tempnext = curr->next;
					evict_bucket(curr);
					curr = tempnext;
				}
			}
			free(temp);
		}
		
		memtab* getTable(unsigned char key)
		{//returns the table pointer to table in buffer on success, nullptr on failure
			bucket_node* head = buffer_directory[int(bitHash(curr_buffer_depth, key))];
			while(head)
			{
				if(head->table->key == key)
					return head->table;
				
				if(head->next)
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
                list_node* temp = curr->next;
                delete curr;
                curr = temp;
            }
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
		void TESTINGBUFFER()
		{
			memtable->printInorder();
			insertIntoBuffer(memtable);
			
			std::cout << int(bitHash(curr_buffer_depth, memtable->key)) << std::endl;
			getTable(memtable->key)->printInorder();
			
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
            while (true) {
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

                if (curr_ind >= num_files) {
                    break;
                }
                curr = build_from_file(get_file_by_ind(curr_ind).c_str());
                curr_ind++;
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
		
		void print_buffer()
		{
			for(int i = 0; i < pow(2, curr_buffer_depth); i++)
			{	
				bucket_node* curr = buffer_directory[i];
				while(curr && curr->table)
				{
					curr->table->printInorder();
					curr = curr->next;
				}
			}
		}
};




//for testing db
int main() {
	srand(time(NULL));

    Database* db = new Database(3);

	db->open("testDB");

    db->put(2, 7);
    db->put(3, 8);
    db->put(4, 9);
    db->put(5, 10);
	db->TESTINGBUFFER();

    /* int result;
    db->get(2, &result);
    std::cout << "result: " << result << std::endl;

	db->close();

	db->open("testDB");
    db->put(1, 2);
	db->close(); */
	
	
    return 0;
}