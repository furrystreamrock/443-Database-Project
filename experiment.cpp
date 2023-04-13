
#include "database.cpp"
#include <chrono>


int main( int argc, char *argv[], char *envp[] ) {

    if (argc != 5) {
        std::cout << "Params are: " << std::endl
            << "Stage of project to test (1 and 3 are valid for this branch)," << std::endl
            << "The number of items to put," << std::endl
            << "Number of gets and scans to perform" << std::endl
            << "Scan length" << std::endl;
        return 0;
    }

    Database* db = new Database();

    //STAGE = 3;
    STAGE = atoi(argv[1]);
    
	db->reset("testDB");
	db->open("testDB");

    int n = atoi(argv[2]);
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

    int gets_n_scans = atoi(argv[3]);

    int gets = gets_n_scans;
    double avg_get = 0.0;
    for (int i = 0; i < gets; i++) {
        std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
        int target = rand() % n; 
        int result;
        db->get(target, &result);
        std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();

        double delta = std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();

        avg_get += delta;

        if (i == 0) {
	        std::cout << "Get target: " << target << " Get result: " << result << std::endl;
        }
    }
    avg_get = avg_get / gets;

	std::cout << "Get Done." << std::endl ;
	std::cout << std::endl ;
    //return 0;

    int scans = gets_n_scans ;
    int scan_len = atoi(argv[4]);
    double avg_scan = 0.0;
    for (int i = 0; i < scans; i++) {

        std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
        int min = rand() % (n - scan_len); 
        int max = min + scan_len; 
        kv_pair* result; int result_len;
        if (i == 0) {
	        std::cout << "minimum: " << min << " maximum: " << max << std::endl;
        }
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


	std::cout << "Num entries: "<< n << std::endl << std::endl;
	std::cout << "Put total time: "<< put_total << "us" << std::endl;
	std::cout << "Put entries/microsecond: "<< n / (put_total/1000000) << "e/us" << std::endl << std::endl;

	std::cout << "Get avg time: "<< avg_get << "us" << std::endl;
	std::cout << "Get avg entries/microsecond: "<< n / (avg_get/1000000) << "e/us" << std::endl << std::endl; 

	std::cout << "Scan avg time: "<< avg_scan << "us" << std::endl;
	std::cout << "Scan avg entries/microsecond: "<< n / (avg_scan/1000000) << "e/us" << std::endl << std::endl;
    
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