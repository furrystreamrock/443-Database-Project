
#include "database.cpp"
#include <chrono>


int main() {
	Database* db = new Database(3);

	db->open("testDB");

    db->put(2, 7);
    db->put(3, 8);
    db->put(4, 9);
    db->put(5, 10);
	std::cout << "buffer testing:" << std::endl;
	db->TESTINGBUFFER();
	std::cout << "----------------------testing done------------------------" << std::endl;
    
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