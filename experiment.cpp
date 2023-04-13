
#include "database.cpp"
#include <chrono>


int main() {

    Database* db = new Database(4);

    STAGE = 3;
    
	db->reset("testDB");
	db->open("testDB");

    int n = 20;
    double put_total = 0.0;
    if (true){
        std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
        for (int i = 0; i < n; i++) {
            db->put(n - i, -i);
        }
        std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
        put_total = std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();
    }

	std::cout << "Put Done." << std::endl;

    int gets = 1;
    double avg_get = 0.0;
    for (int i = 0; i < gets; i++) {
        std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
        int target = 13; //rand() % (n - 4); 
        int result;
        db->get(target, &result);
        std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();

        double delta = std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();

        avg_get += delta;

	    std::cout << "Get target: " << target << " Get result: " << result << std::endl;
    }
    avg_get = avg_get / gets;

	std::cout << "Get Done." << std::endl ;
	std::cout << std::endl ;
    //return 0;

    int scans = 1;
    int scan_len = 10;
    double avg_scan = 0.0;
    for (int i = 0; i < scans; i++) {

        std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
        int min = 5; //rand() % (n - scan_len - 4); 
        int max = min + scan_len; 
        kv_pair* result; int result_len;
	    std::cout << "minimum: " << min << " maximum: " << max << std::endl;
        db->scan(min, max, &result, &result_len);
        std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();

        double delta = std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();

        avg_scan += delta;

        if (i == 0) {
	        std::cout << "Scan result: ";
            for (int j = 0; j < result_len; j++) {
	            std::cout << result[j].key << ";";
            }
	        std::cout << std::endl;
        }
    }
    avg_scan = avg_scan / scans;
	std::cout << "Scan Done." << std::endl;


	std::cout << "Num entries: "<< n << std::endl;
	std::cout << "Put Total Time: "<< put_total << "ms" << std::endl;
	std::cout << "Get Avg Time: "<< avg_get << "ms" << std::endl;
	std::cout << "Scan Avg Time: "<< avg_scan << "ms" << std::endl;
    
	db->close();

    /*

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
	*/
	
    return 0;
}