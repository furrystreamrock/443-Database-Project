#include <algorithm>
#include <fstream> 
#include <sstream>
#include <string>
#include <iostream>
#include "btree.cpp"
#include <random> 
#include <cmath>

int STAGE = 0;

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

//static const int SST_BYTES = 4096 - sizeof(SST) - 30; /for reference of values
//static const int MAX_ENTRIES = SST_BYTES / sizeof(kv_pair);


class Database {
    std::string database_name;
	
	//stuff for the buffer
	bool first;
	int search_style;
	const int min_buffer_depth = 2; 
	int max_buff_depth = 4;
	int curr_buffer_depth;
	int curr_buffer_entries;
	int evict_policy; //NOTE TO SELF######: assign policies to ints. (0 is lru, 1 is clock, etc)
	node_dll* lru_head;
	node_dll* lru_tail;
	node_dll* clock_pointer;
	
    int memtable_size;
    memtab* memtable;
	bucket_node ** buffer_directory;
	
	SST_directory* SST_DIR;//manages all the ssts
	
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
		{//initialize buffer hyperparameters and allocate memory:
		//evict_policy: 1 for clock, 0 for LRU
		//buffer depth minimum is set to 2, maximum may be set under max_buff_depth parameter.
			buffer_directory = (bucket_node**)(malloc(pow(2,min_buffer_depth) * sizeof(bucket_node*)));
			curr_buffer_depth = min_buffer_depth;
			for(int i = 0; i < pow(2, curr_buffer_depth); i++)
			{//assign memory for the initial buffer buckets
				buffer_directory[i] = new bucket_node();
			curr_buffer_entries = 0;
			}
			evict_policy = 0;
			lru_head = nullptr;
			lru_tail = nullptr;
			clock_pointer = nullptr;
		}
		
		
		void insertIntoBuffer(SST* sst, bool reinsert = false)
		{//inserts table into buffer, evicts if needed
		//should be done on pages we know aren't already in the buffer
		//Also, the passed SST* sst should already be MALLOCED!!!!
				
			if(curr_buffer_entries >= pow(2, curr_buffer_depth) * 1)//'full' capacity threshold
			{
				if(curr_buffer_depth >= max_buff_depth)
					evict();
				else
					doubleBufferSize();
			} 

				
			curr_buffer_entries++;
			unsigned long hash = bitHash(curr_buffer_depth, sst->key);
			bucket_node* curr_head = buffer_directory[hash];
			////std::cout << curr_buffer_depth << std::endl;
			//std::cout << "Buffer pages: " << curr_buffer_entries << std::endl;
			//std::cout << "Inserting SST: " << sst->key << " at hash: "<<  int(hash) << std::endl;
			if(!curr_head)
				return;
			while(curr_head->next)
				curr_head = curr_head->next;
			curr_head->sst = sst;//sst should already be malloced.
			
			/* curr_head->sst->data = (kv_pair*)(malloc(MAX_ENTRIES*sizeof(kv_pair)));
			std::memcpy(curr_head->sst->data, sst->data, MAX_ENTRIES*sizeof(kv_pair)); */

			curr_head->next = new bucket_node();
			curr_head->next->prev = curr_head;
			
			//printBuckets();
			//LRU
			if(evict_policy == 0)
			{
				lruUpdate(curr_head);
				
			}
			//clock
			if(evict_policy == 1)
			{
				if(!clock_pointer)
				{
					clock_pointer = new node_dll();
					clock_pointer->next = clock_pointer;
					clock_pointer->prev = clock_pointer;
					clock_pointer->target = curr_head;
				}
				else
				{//insert it write behind the clock pointer in the circle queue
					node_dll* before = clock_pointer->prev->prev;
					node_dll* n = new node_dll();
					n->target = curr_head;
					
					before->next = n;
					n->prev = before;
					n->next = clock_pointer;
					clock_pointer->prev = n;
				}
			}

			
		}	
		void evict()
		{//policy dependant
			if(curr_buffer_entries-1 <= pow(2, curr_buffer_depth) *0.3)//less than 30% full after evicting this page
				halveBufferSize();
			
			if(evict_policy == 0)
				lruEvict();
			if(evict_policy == 1)
				clockEvict();
			
			curr_buffer_entries--;
		}
		
		void lruEvict()
		{//remove the tail of lru, should free all associated memory in that page as well
			//std::cout << "LRU Evicting " << lru_tail->target->sst->key << " in bucket: " << bitHash(curr_buffer_depth, lru_tail->target->sst->key) << std::endl;
			if(!lru_tail)
				std::cerr << "Warning! tried to evict in empty buffer." << std::endl;
			
			if(!(lru_tail->target->prev))//first in the bucket.
			{
				buffer_directory[bitHash(curr_buffer_depth, lru_tail->target->sst->key)] = lru_tail->target->next;
				lru_tail->target->next->prev = nullptr;
			}
			else
			{
				lru_tail->target->prev->next = lru_tail->target->next;
				lru_tail->target->next->prev = lru_tail->target->prev;
			}

			flush(lru_tail->target->sst);//write sst to file
			cleanBucket(lru_tail->target);//cleanup memory 
			
			delete(lru_tail->target);
			node_dll* temp = lru_tail->prev;
			

			delete(lru_tail);
			lru_tail = temp;
			if(lru_tail)
				lru_tail->next = nullptr;

			
		}
		void lruUpdate(bucket_node* target)
		{//new get or insert updates a page
			//first check if its already in queue, if it is, refresh it to the front of the queue
			
			if(target->lru_node)
			{
				node_dll* n = (node_dll*) (target->lru_node);
				if(!n->prev)//n is the very first node
					return;
					
				if(n->prev)
					n->prev->next = n->next;
				if(n->next)
					n->next->prev = n->prev;
								
				n->next = lru_head;
				n->prev = nullptr;
				lru_head = n;
				return;
			}
			
			node_dll* new_lru = new node_dll();
			new_lru->next = lru_head;
			if(new_lru->next)
				new_lru->next->prev = new_lru;
			new_lru->target = target;
			lru_head = new_lru;
			if(!lru_tail)
				lru_tail = new_lru;
			target->lru_node = new_lru;
		}
		void clockEvict()
		{
			//std::cout << "Clock Evicting " << clock_pointer->target->sst->key << " in bucket: " << bitHash(curr_buffer_depth, clock_pointer->target->sst->key) << std::endl;
			if(!clock_pointer->target->prev)//first in the bucket.
			{
				buffer_directory[bitHash(curr_buffer_depth, clock_pointer->target->sst->key)] = clock_pointer->target->next;
				clock_pointer->target->next->prev = nullptr;
			}
			else
			{
				clock_pointer->target->prev->next = clock_pointer->target->next;
				clock_pointer->target->next->prev = clock_pointer->target->prev;
			}
			//handle the clock circle queue
			//at least 2 pages: 
			while(clock_pointer->target->clockbit)
			{//move the hand foward, looking for first 0 bit 
				clock_pointer->target->clockbit = false;
				clock_pointer = clock_pointer->next;
			}
			
			clock_pointer->prev->next = clock_pointer->next;
			clock_pointer->next->prev = clock_pointer->prev;
			//process the bucket's delete
			node_dll* temp = clock_pointer->next;
			flush(clock_pointer->target->sst);//write to file
			cleanBucket(clock_pointer->target);//cleanup disk space
			delete(clock_pointer);
			clock_pointer = temp;

			
		}
		void cleanBucket(bucket_node * n)
		{//evict a certain memtab
			free(n->sst->data);
			free(n->sst);
		}
		
		void doubleBufferSize()
		{
			//std::cout << "Doubling buffer capacity, new depth: " << curr_buffer_depth + 1 << std::endl;
			if(curr_buffer_depth >= max_buff_depth)
			{
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
						//std::cout << "Re-inserting SST: " << curr->sst->key << std::endl;
						reinsertBucket(curr, new_buffer_directory);
					}
					else
						delete(curr);//dead end marker
					curr = temp;
				}
			}
			free(buffer_directory);
			buffer_directory = new_buffer_directory;
		}
		
		void halveBufferSize()
		{
			//std::cout << "Halving buffer capacity, new depth: " << curr_buffer_depth - 1 << std::endl;
			if(curr_buffer_depth <= min_buffer_depth)
			{
				std::cerr << "Warning, can't double buffer size" << std::endl;
				return;
			}
			curr_buffer_depth--;
						
			//new directory with half the size.
			bucket_node** new_buffer_directory = (bucket_node**)(malloc(pow(2,curr_buffer_depth) * sizeof(bucket_node*)));
			for(int i = 0; i < pow(2, curr_buffer_depth); i++)
			{	
				new_buffer_directory[i] = new bucket_node();
			}
			//re insert previous directory into the new one.
			
			for(int i = 0; i < pow(2, curr_buffer_depth+1); i++)
			{	
				bucket_node* curr = buffer_directory[i];
				while(curr)
				{
					bucket_node* temp = (bucket_node*)curr->next;
					if(!curr)//somehow segfaulting without this line
						break;
					if(curr->sst)
					{
						//std::cout << "Re-inserting SST: " << curr->sst->key << std::endl;
						reinsertBucket(curr, new_buffer_directory);
					}
					else
						delete(curr);//dead end marker
					curr = temp;
				}
			}
			free(buffer_directory);
			buffer_directory = new_buffer_directory;
		}
		
		reinsertBucket(bucket_node* b, bucket_node** dir)
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
				{
					if(evict_policy == 0)
						lruUpdate(head);
					else
						head->clockbit = true;
					/* node_dll* a = lru_head;//show LRU queue for debug
					while(a)
					{
						//std::cout << a->target->sst->key << "|";
						////std::cout << bitHash(curr_buffer_depth, a->target->sst->key) << "|";
						a = a->next;
					}
					//std::cout << std::endl; */
					
					return head->sst;
				}
				head = head->next;
			}
			return nullptr;
		}
		

//----------------------------------buffer methods end-------------------------------------------------	
		SST* fetch(unsigned long key)
		{//fetch the SST for from file, and put it into the buffer
			std::string filename = std::to_string(key) + ".bin";
			//std::cout << "fetching: " <<  filename << std::endl;
			std::ifstream f(filename, std::ios::out | std::ios::binary);
			if(!f.is_open())
			{//if this happens, the db instance should exit instantly to preserve data pages
				std::cerr << "CRITICAL ERROR, Fetch for key: " << key << " failed" << std::endl;
				exit(1);
			}
			SST* sst = new SST();
			sst->data = (kv_pair*)malloc(MAX_ENTRIES*sizeof(kv_pair));
			f.read((char *)&(sst->key), sizeof(unsigned long));
			f.read((char *)&(sst->entries), sizeof(int));
			f.read((char *)&(sst->minkey), sizeof(int));
			f.read((char *)&(sst->maxkey), sizeof(int));
			for(int i = 0 ; i < sst->entries; i++)
			{
				f.read((char*)(&(sst->data[i].key)), sizeof(int));
				f.read((char*)(&(sst->data[i].value)), sizeof(int));
			}
			f.close();
			
			//std::cout << "Just fetched SST:  Key: " << sst->key << " Entries " << sst->entries << std::endl;
			return sst;
		}
		
		void flush(SST* sst)
		{//flush SST to file, write it as a binary file: formatting specified in project document. Note** this does not clean up the memory for SST, just writes.
			//write order: key, entries, min, max, data. All densely written
			std::string filename = std::to_string(sst->key) + ".bin";
			//std::cout << "flushing: " <<filename << std::endl;
			std::ofstream f(filename, std::ios::out | std::ios::binary);
			if(!f.is_open())
			{//if this happens, the db instance should exit instantly to preserve data pages
				std::cerr << "CRITICAL ERROR, Fetch for key: " << sst->key << " failed" << std::endl;
				exit(1);
			}
			
			f.write((char *)(&(sst->key)), sizeof(unsigned long));
			f.write((char *)(&(sst->entries)), sizeof(int));
			f.write((char *)(&(sst->minkey)), sizeof(int));
			f.write((char *)(&(sst->maxkey)), sizeof(int));
			for(int i = 0; i < sst->entries; i++)
			{
				f.write((char*)(&(sst->data[i].key)), sizeof(int));
				f.write((char*)(&(sst->data[i].value)), sizeof(int));
			}
		
			
			f.close();
		}
		
		int bin_search(SST* sst, int key, bool* found)
		{//Binary search to find KV pair in given SST, sets found to true on success, false otherwise
			int bottom = 0; 
			int top = sst->entries-1;
			*found = false;
			while(bottom <= top)
			{
				int mid =( bottom + top)/2;
				if(sst->data[mid].key == key)
				{
					*found = true;
					return sst->data[mid].value;
				}
				
				if(sst->data[mid].key< key)
					bottom = mid + 1;
				else
					top = mid - 1;
			}
			return 1;
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

		Database(int cap)
		{
			srand(time(NULL));
			initializeBuffer();
			SST_DIR = new SST_directory();
			search_style = 0;
			//std::cout << "Database Initiazlied" << std::endl;
			first = true;
			put(0, 0);//key 0 reserved for special function in this DB. use >  positive keys only.
		}
		void setEviction(int eviction)
		{
			evict_policy = eviction;
		}
        int get(int key, bool* found) 
		{
			bool sst_found;
			bool in_buffer;
			unsigned long pagekey = SST_DIR->getSSTKey(key, &sst_found, &in_buffer);
			if(!sst_found)
				return 0;
			SST* target;
			if(!in_buffer)
			{//retrieve the appropriate page and place in buffer, update the SST_directory to let it know new page in buffer.
				target = (SST*)malloc(sizeof(SST));
				memcpy(target, fetch(pagekey), sizeof(SST));
				insertIntoBuffer(target);
				SST_DIR->update_sst_node(target);
			}
			else
				target = getSST(pagekey);
			
			int result = bin_search(target, key, found);
			return result;
        }
		
        void put(int key, int val)
		{
			if(first)
			{//exception to pattern to insert the first entry ever:
				first = false;
				kv_pair* a = new kv_pair(key, val);
				insertIntoBuffer(SST_DIR->put(a));
				delete(a);
				return;
			}
			//first make sure the target page is loaded
			unsigned long target_key = SST_DIR->getInsertKey(key);
			if(!getSST(target_key))
			{//the target sst is not in buffer, need to fetch.
				SST* load = (SST*)malloc(sizeof(SST));
				std::memcpy(load, fetch(target_key), sizeof(SST));
				insertIntoBuffer(load);
				SST_DIR->update_sst_node(load);
			}
			//std::cout << "checkpoint 1" << std::endl;
			kv_pair* a = new kv_pair(key, val);
			SST* b = SST_DIR->put(a);
			//std::cout << "checkpoint 1.2" << std::endl;
			if(b)
			{//a new SST was made by the insertion, for now, just flush this new one to file and dont bother with it.
				flush(b);
				//std::cout << "checkpoint 1.3" << std::endl; 
				free(b->data);
				free(b);
				//std::cout << "checkpoint 1.4" << std::endl;
			}//otherwise insertion was done into an non-full table so we dont need to do more here.
			//std::cout << "checkpoint 1.5" << std::endl;
			delete(a);
			bool dog;
			//std::cout << "checkpoint 2" << std::endl;
			for(int i = 0; i < pow(2, curr_buffer_depth); i++)
			{
				bucket_node* buk = buffer_directory[i];
				while(buk->next)
				{
					//print_sst(buk->sst);
					buk = buk->next;
				}
			}
		}
            

        void scan() 
		{//TODO REimplment this using page directory and buffer!!
            
        }
            
		void testReadWrite()
		{
			//std::cout << "Binary Read/Write check" << std::endl;
			kv_pair* kv1 = new kv_pair(0, 0);
			kv_pair* kv2 = new kv_pair(1 ,1);
			kv_pair* kv3 = new kv_pair(2, 2);
			kv_pair* kv4 = new kv_pair(3, 3);
			kv_pair* kv5 = new kv_pair(4 ,4);
			kv_pair* kv6 = new kv_pair(5, 5);
			SST* test = new SST();
			test->data = (kv_pair*)malloc(MAX_ENTRIES*sizeof(kv_pair));
			
			SST_DIR->sstInsert(test, kv1);
			SST_DIR->sstInsert(test, kv2);
			SST_DIR->sstInsert(test, kv3);
			SST_DIR->sstInsert(test, kv4);
			SST_DIR->sstInsert(test, kv5);
			SST_DIR->sstInsert(test, kv6);
			test->key= 100000;
			//std::cout <<"FLUSHING" << std::endl;
			flush(test);
			//std::cout << "FETCHING" << std::endl;
			SST* test_result = fetch(test->key);
			print_sst(test_result);
			
		}

        void reset(const char* database_name) {
            this->database_name = database_name;

            std::ifstream f(get_db_file_name());
	        if(!f.is_open()) {
                
            } else {
                //std::cout << "Deleting all. " << std::endl;
                //load the db
                std::string temp;
                std::getline(f, temp);

                num_files = stoi(temp);
                
                std::getline(f, temp);
                int buffer_empty = stoi(temp);

                if (buffer_empty == 0) { 
                    num_files++;
                }

                remove(get_db_file_name().c_str());

                for (int i = 0; i < num_files; i++) {
                    remove(get_file_by_ind(i).c_str());
                }
            }

            this->database_name = "";
        }

        int open(const char* database_name) {
			return 0;
        }
	
        void close()
		{
 
        }
		
		void TESTINGBUFFER(int eviction, int iterations, int maxdepth)
		{//stress test buffer
			srand(time(NULL));
			evict_policy = eviction;
			unsigned int test_key = 0;
			if(maxdepth>2)
				max_buff_depth = maxdepth;
			for(int i = 0; i < iterations; i++)
			{
				SST* tab0 = new SST();
				//std::cout<< "Created table :" << tab0->key << std::endl;
				tab0->data = (kv_pair*)(malloc(MAX_ENTRIES * sizeof(kv_pair)));
				for(int j = 0; j < 10; j++)
				{
					tab0->data[j].key = i*100+j;
					tab0->data[j].value = i*100+j;
				}
				//std::cout << "----------------------INSERTING: " << i << "----------------------" << std::endl;		
				insertIntoBuffer(tab0);
				free(tab0->data);
				
				if(i == 3)
				{
					test_key = tab0->key;
					
				}
				
				if(i%5 == 0 && i >= 3)
					getSST(test_key);
				delete(tab0);
				
				
			}
			
			//std::cout << "Finished " << iterations << " insertions" << std::endl;

			SST* getResult =  getSST(test_key);
			
			if(!getResult)
			{
				//std::cout << "test_key SST was evicted" << std::endl;
				return;
			}
			//std::cout << "Testing to retrieve table: " << getResult->key << std::endl;
			for(int i = 0; i < 10; i++)
			{
				//std::cout << getResult->data[i].value << std::endl;
			}
		}
};

