
#include "database.cpp"
#include <chrono>


int main( int argc, char *argv[], char *envp[] ) {

    Database* db = new Database();

    STAGE = 3;
    
	db->reset("testDB");
	db->open("testDB");

    int n = 250000000;
    int gets = 3;
    int scans = 3;
    int scan_len = 100;

    double results_put[6];
    double results_get[6];
    double results_scan[6];

    int iter = 0;
    int prev = 0;
    int inputs = 10000;
    bool finish = false;
    while (finish == false) {

        double put_total = 0.0;
        std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
        for (int i = prev; i < inputs; i++) {
            db->put(n - i, -i);

            if (i >= n - 1) {
                finish = true;
                break;
            }
        }
        std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
        put_total = std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();
        results_put[iter] = put_total;

        double avg_get = 0.0;
        for (int i = 0; i < gets; i++) { 
            std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
            int target = rand() % inputs; 
            int result;
            db->get(target, &result);
            std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
            
            double delta = std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();
            avg_get += delta;
        }
        results_get[iter] = avg_get/gets;

        double avg_scan = 0.0;
        for (int i = 0; i < scans; i++) { 
            std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
            int min = rand() % (inputs - scan_len); 
            int max = min + scan_len; 
            kv_pair* result; int result_len;
            // if (i == 0) {
            //     std::cout << "minimum: " << min << " maximum: " << max << std::endl;
            // }
            db->scan(min, max, &result, &result_len);
            std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();

            double delta = std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count();

            avg_scan += delta;
        }
        results_scan[iter] = avg_scan/scans;

        iter++;
        prev = inputs;
        inputs = inputs * 10;
    }

    for (int i = 0; i < 6; i++) {
        std::cout << "Stage: "<< i << std::endl << std::endl;
        std::cout << "Put entries/microsecond: "<< n / (results_put[i]/1000000) << "e/s" << std::endl << std::endl;

        std::cout << "Get avg entries/microsecond: "<< n / (results_get[i]/1000000) << "e/s" << std::endl << std::endl; 

        std::cout << "Scan avg entries/microsecond: "<< n / (results_scan[i]/1000000) << "e/s" << std::endl << std::endl;
    }
    
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