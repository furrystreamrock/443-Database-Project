
#include "database.cpp"
#include <chrono>


int main( int argc, char *argv[], char *envp[] ) {

    Database* db = new Database();
    int foo [10] = { 39, 10, 492, 423, 398, 20, 1, 4, 23, 4000 };
    int fi [10] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };

    STAGE = 1;
    
	db->reset("simpletestDB");
	db->open("simpletestDB");
    for (int i = 0; i < 10; i++) {
        db->put(foo[i], fi[i]);
    }
	db->close();
	db->open("simpletestDB");
    int result;
    db->get(20, &result);
    std::cout << "Get target: " << 6 << " Get result: " << result << std::endl;

    int min = 20; 
    int max = 50; 
    kv_pair* scan_result; int result_len;
    db->scan(min, max, &scan_result, &result_len);

    std::cout << "Scan target: 20;23;39;";
    std::cout << "Scan result: ";
    for (int i = 0; i < result_len; i++) {
        std::cout << scan_result[i].key << ";";
    }
    std::cout << std::endl;

    if (STAGE == 3) {
        std::cout << "Updated value of key 20 to be 66. " << std::endl;
        db->update(20, 66);
        int result2;
        db->get(20, &result2);
        std::cout << "Get target: " << 66 << " Get result: " << result << std::endl;

        std::cout << "Deleted 20. " << std::endl;
        db->del(20);
        db->get(20, &result2);
        std::cout << "Has been deleted: " << (result2 == TOMBSTONE) << std::endl;
    }

	db->close();

    
	
    return 0;
}