#include "database.cpp"
#include <iostream>
#include <chrono>
#include <unistd.h>

int main() 
{
	Database* db = new Database(MAX_ENTRIES);
	db->open("bufferTestingDB");
	srand(time(NULL));
	
	
	//control which experiment to run: 0 for random page accesses, 1 for sequential page accesses.
	int experiment = 1;
	db->setEviction(1);//set the eviction policy; 0 for LRU, 1 for Clock.
	
	
	auto begin = std::chrono::high_resolution_clock::now();
	if(experiment == 0)
		for(int i = 0; i < 10000; i++)
		{
			//std::cout << "----------------------------------------------Put " << i << " -------------------------------------------------------" <<std::endl;
			int k = rand()%10000+1;
			int v = rand()%10000;
			//std::cout << "Inserting k\\v: " << k <<"\\" << v << std::endl; 
			db->put(k, v);
		}
	else
		for(int i = 0; i < 10000; i++)
		{
			//std::cout << "----------------------------------------------Put " << i << " -------------------------------------------------------" <<std::endl;

			//std::cout << "Inserting k\\v: " << k <<"\\" << v << std::endl; 
			db->put(i, i);
		}
	
	auto end = std::chrono::high_resolution_clock::now();
	std::cout << std::chrono::duration_cast<std::chrono::nanoseconds>(end-begin).count() << "ns" << std::endl;

	//std::cout << "value at " << store << " is " << db->get(store, &found) << std::endl;
	//SST_directory* sst_dir = new SST_directory();
	
	
	//sst_dir->testInsert(); //test tje insert functionality of page directory
	
	return 0;
    /* int result;
    db->get(2, &result);
    std::cout << "result 1: " << result << std::endl;

    kv_pair* results; 
    int results_len = 0; 
    db->scan(5, 1000, &results, &results_len);
    std::cout << "result 2: " << results->key << " w/ len " << results_len << std::endl;

	db->close();

	db->open("testDB");
    db->put(1, 2);
	db->close(); */
	
	
 
}



/*memtable->printInorder();
			std::cout<< "--------------------Insert 1--------------------" << std::endl;
			insertIntoBuffer(memtable);
			
			std::cout<< "--------------------Insert 2--------------------" << std::endl;
			memtab* tab2 = new memtab(10);
			tab2->insert(22,5);
			tab2->insert(23,7);
			tab2->insert(210,10);
			insertIntoBuffer(tab2);
			
			std::cout<< "--------------------Insert 3--------------------" << std::endl;
			memtab* tab3 = new memtab(10);
			tab3->insert(32,5);
			tab3->insert(33,7);
			tab3->insert(310,10);
			insertIntoBuffer(tab3);
			
			std::cout<< "--------------------Insert 4--------------------" << std::endl;
			memtab* tab4 = new memtab(10);
			tab4->insert(432,5);
			tab4->insert(433,7);
			tab4->insert(4310,10);
			insertIntoBuffer(tab4);
			
			std::cout<< "--------------------Insert 5--------------------" << std::endl;
			memtab* tab5 = new memtab(10);
			tab5->insert(532,5);
			tab5->insert(533,7);
			tab5->insert(5310,10);
			insertIntoBuffer(tab5);
			std::cout << "CHECKPOINT 5" << std::endl;
			//std::cout << int(bitHash(curr_buffer_depth, memtable->key)) << std::endl;
			
			getTable(memtable->key)->printInorder();
			getTable(tab3->key)->printInorder();
			std::cout<<"----------------------buffer test done----------------------" <<std::endl;		
			*/