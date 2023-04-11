#include <iostream>
#include <algorithm>
#include <fstream> 
#include <sstream>
#include <string>
#include <iostream>
#include <random>
#include <time.h>  
#include <cmath>
#include "objs.cpp"

const int LSM_SIZE = 2;

struct LSM_NODE {
    //LSM_NODE* parent;
    const char* file_name;

    int num_children;
    LSM_NODE* children[LSM_SIZE];

    int depth;

    LSM_NODE()
    {
        this->depth = 0;
        this->num_children = 0;
        //this->parent = nullptr;

        for (int i = 0; i < LSM_SIZE; i++) {
            this->children[i] = nullptr;
        }
    }
};


class lsmtree {
    LSM_NODE* root;

    const char* db_name;
    int num_files;

	private:
        void read(){
            
        }

        void merge(kv_pair* x, int x_len, kv_pair* y, int y_len){
            kv_pair* z = (kv_pair*)( malloc(sizeof(kv_pair) * (x_len + y_len)) );
            for (int i = 0; i < x_len + y_len; i++) {

            }
        }

    public:
        std::string get_file_by_ind(int i){
            return std::to_string(i) + (std::string)(".sst");
        }

        std::string get_next_file_name(){
            return std::to_string(num_files) + (std::string)(".sst");
        }

        std::string get_db_file_name(){
            return db_name + (std::string)(".db");
        }

        bool flush(){
            return false;
        }

        bool open_sst(){
            return false;
        }

        lsmtree(const char* db_name){
            this->root = nullptr;
            this->db_name = db_name;
            this->num_files = 0;
        }


};