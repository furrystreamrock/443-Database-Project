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
#include "bloomfilter.cpp"
#include <iomanip>
#include <climits>
#include <limits.h>

const int PAGE_LENGTH = 250000;
const int LSM_SIZE = 2; //DO NOT CHANGE
const int buf_len = 0;

const int TOMBSTONE = INT_MIN;

struct LSM_NODE {  
    //LSM_NODE* parent;

    int num_files;
    int page_origin[LSM_SIZE]; //file number
    int page_length[LSM_SIZE]; //file length
    
    LSM_NODE* child;

    //TODO: figure out how to do the buffer as shown in lecture slides
    kv_pair buffer[buf_len];

    LSM_NODE()
    {
        this->num_files = 0;
        this->child = nullptr;

        for (int i = 0; i < LSM_SIZE; i++) {
            this->page_origin[i] = -1;
            this->page_length[i] = -1;
        }
    }

    void add_file(int file_number, int file_length){
        this->page_origin[num_files] = file_number;
        this->page_length[num_files] = file_length;
        this->num_files++;
    }

    void clear(){
        this->num_files = 0;
    }
};

//TODO: cuz LSM_SIZE is fixed to be 2 we should be able to do powers of 2
const int BITMAP_SIZE = 64;
class lsmtree {
    LSM_NODE* root;

    const char* db_name;
    int num_files; //AKA: number of pages being stored
    bool file_bitmap[BITMAP_SIZE]; //for actually finding the individual files

	private:

        void clear() {

        }

        bool read_next(std::ifstream* f, int* key, int* val){
            std::string k, v;
            if (std::getline(*f, k, ','))
            {
                std::getline(*f, v);
                
                *key = stoi(k);
                *val = stoi(v);
                
                return true;
            } else {
                return false;
            }
        }

        bool write(const char* filename, kv_pair* list, int len){
			std::stringstream entries;
			std::ofstream output((std::string)filename);

            //metadata
            write_meta(&output, 1);
			for(int i = 0; i < len; i++)
			{
				entries << list[i].key << "," << list[i].value << "\n";
			}
			std::string file_out = entries.str();
			output << file_out;
			output.close();

            file_bitmap[num_files] = true;
            num_files++;

            return true;
        }

        //read metadata; does NOT close file
        bool get_meta(std::ifstream* f, int* sst_num_pages, uint32_t* bitmap){
            std::string str_bitmap;
            if (std::getline(*f, str_bitmap, ','))
            {
                if (bitmap != nullptr) {
                    *bitmap = stoul(str_bitmap);
                }
            }
            
            std::string str_num_pages;
            if (std::getline(*f, str_num_pages))
            {
                *sst_num_pages = stoi(str_num_pages);
            }

            return true;
        }

        //write the metadata to an sst
        bool write_meta(std::ofstream* output, int sst_num_pages){
			std::stringstream entries;
			entries << "4294967295" << "," << sst_num_pages << "\n";
			std::string meta_out = entries.str();
			(*output) << meta_out;

            return true;
        }

    public:
        /**
         * sort-merge; does not modify x or y
         *
         * @param x newer array of pairs
         * @param x_len x array length
         * @param y older array of pairs
         * @param y_len y array length
         * @param z_len sets this to length of merged list
         * @return ptr to merged list
         */
        static kv_pair* merge_list(kv_pair* x, int x_len, kv_pair* y, int y_len, int* z_len){
            kv_pair* z = (kv_pair*)( malloc(sizeof(kv_pair) * (x_len + y_len)) );
            int i = 0;
            int j = 0;
            for (int k = 0; k < x_len + y_len; k++) {
                if (i >= x_len) {
                    z[k].key = y[j].key;
                    z[k].value = y[j].value;
                    j++;
                    *z_len = (*z_len) + 1;
                } else if (j >= y_len) {
                    z[k].key = x[i].key;
                    z[k].value = x[i].value;
                    i++;
                    *z_len = (*z_len) + 1;
                } else if (x[i].key == y[j].key) {
                    z[k].key = x[i].key;
                    z[k].value = x[i].value;
                    i++;
                    j++;
                    *z_len = (*z_len) + 1;
                } else if (x[i].key < y[j].key) {
                    z[k].key = x[i].key;
                    z[k].value = x[i].value;
                    i++;
                    *z_len = (*z_len) + 1;
                } else {
                    z[k].key = y[j].key;
                    z[k].value = y[j].value;
                    j++;
                    *z_len = (*z_len) + 1;
                }
                
            }
            //std::cout << std::endl;

            return z;
        }

        //deprecated: for opening an entire sst
        static kv_pair* load_sst(const char* filename, int* len){
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


        std::string get_file_by_ind(int i){
            return std::to_string(i) + (std::string)(".sst");
        }

        std::string get_next_file_name(){
            return std::to_string(num_files) + (std::string)(".sst");
        }

        std::string get_db_file_name(){
            return db_name + (std::string)(".db");
        }

        std::string get_lsmt_file_name(){
            return db_name + (std::string)(".lsmt");
        }

        int get_num_pages(){
            return num_files;
        }


        /**
         * from the page number, find the sst file ID and the offset from the beginning of the file in num pages
         *
         * @param page_num the page number sought
         * @param page_origin sets this to file ID
         * @param page_offset sets this to offset
         * @return true on success
         */
        bool get_page(int page_num, int* page_origin, int* page_offset) {
           //std::cout << "getting page: " << page_num << std::endl;
            int curr = page_num;
            int offset = 0;
            while (file_bitmap[curr] == false) {
                if (curr < 0) {
                    int* a;
                    *a = 2;
                    return false;
                }
                offset++;
                curr--;
            }

            //std::cout << "found page: " << page_num << std::endl;
            *page_origin = curr;
            *page_offset = offset;

            return true;
        }
        
        /**
         * checks a page's filter to see if it might contain the key
         *
         * @param page_num the page number sought
         * @param key the key to check
         * @return true if the key may be inside
         */
        bool check_filter(int page_num, int key){
            int sst_num_pages;
            uint32_t bitmap;
            read_meta(page_num, &sst_num_pages, &bitmap);

            bloomfilter* filter = new bloomfilter();
            filter->set_bitmap(bitmap);

            return filter->check(key);
            
        }

        /**
         * gets the bitmap + num pages of the page
         *
         * @param page_num the page number sought
         * @param sst_num_pages sets as number of pages in the file
         * @param bitmap sets as the filter bitmap
         * @return true on success
         */
        bool read_meta(int page_num, int* sst_num_pages, uint32_t* bitmap) {
            int page_origin; 
            int page_offset;
            get_page(page_num, &page_origin, &page_offset);

            std::ifstream f(get_file_by_ind(page_origin).c_str());
            if(!f.is_open())
            {
                std::cout << "Failed to open metadata for file w/ id: " << page_origin << std::endl;
                return false;
            }

            get_meta(&f, sst_num_pages, bitmap);
            f.close();

            return true;
        }

        /**
         * gets the data in the page
         *
         * @param page_num the page number sought
         * @param page_data sets as the ptr to the data (sorted array)
         * @param data_len sets as the array length
         * @return true on success
         */
        bool read_page(int page_num, kv_pair** page_data, int* data_len) {
            int page_origin; 
            int page_offset;
            get_page(page_num, &page_origin, &page_offset);

            std::ifstream f(get_file_by_ind(page_origin).c_str());
            if(!f.is_open())
            {
                std::cout << "Failed to open data for file w/ id: " << page_origin << std::endl;
                return false;
            }

            //skip past metadata
            int num_pages;
            get_meta(&f, &num_pages, nullptr);

            //count and to the beginning
            int num_lines = std::count(std::istreambuf_iterator<char>(f), 
                std::istreambuf_iterator<char>(), '\n');
            f.clear();
            f.seekg(0);

            //skip past metadata
            get_meta(&f, &num_pages, nullptr);

            kv_pair* data = (kv_pair*)( malloc(sizeof(kv_pair) * (PAGE_LENGTH)) );

            int total_lines = 0;
            int read_lines = 0;
            int curr_lines = 0;
            int curr_page = 0;
            int key;
            int val;
            while (read_next(&f, &key, &val)){
                if (curr_page == page_offset) {
                    //std::cout << "pair: " << key << " val: " << val << std::endl;
                    data[curr_lines].key = key;
                    data[curr_lines].value = val;
                    ++read_lines;
                } else if (curr_page > page_offset) {
                    break;
                }
                ++total_lines;
                ++curr_lines;
                if (curr_lines == PAGE_LENGTH) {
                    curr_lines = 0;
                    curr_page++;
                }
                if (curr_lines >= num_lines){
                    break;
                }
            }

            *data_len = read_lines;
            *page_data = data;

			f.close();
            return true;
        }


        /**
         * merges two ssts
         *
         * @param file_num_x the newer page to be merged
         * @param file_num_y the older page to be merged
         * @param new_len sets as the new files length
         * @return true on success
         */
        bool merge_file(int file_num_x, int file_num_y, int* new_len){
            const int buffer_size = 4;

            //x is the newer SST
            int next_x_key;
            int next_x_val;
            bool next_x_empty = true;
            
            int next_y_key;
            int next_y_val;
            bool next_y_empty = true;

            using namespace std;
            //========================= open x =========================
            std::ifstream f_x(get_file_by_ind(file_num_x).c_str());
            if(!f_x.is_open())
            {
                std::cout << "Failed to open file 1 w/ id: " << file_num_x << std::endl;
                return false;
            }

            int num_lines_x = std::count(std::istreambuf_iterator<char>(f_x), 
                std::istreambuf_iterator<char>(), '\n');
            num_lines_x--;

            f_x.clear();
            f_x.seekg(0);
            
            int num_pages_x;
            get_meta(&f_x, &num_pages_x, nullptr);

            //========================= open y =========================
            std::ifstream f_y(get_file_by_ind(file_num_y).c_str());
            if(!f_y.is_open())
            {
                std::cout << "Failed to open file 2 w/ id: " << file_num_y << std::endl;
                return false;
            }

            int num_lines_y = std::count(std::istreambuf_iterator<char>(f_y), 
                std::istreambuf_iterator<char>(), '\n');

            f_y.clear();
            f_y.seekg(0);

            int num_pages_y;
            get_meta(&f_y, &num_pages_y,  nullptr);

            //========================= open z =========================
            char const* temp_file_name = get_next_file_name().c_str();
            std::ofstream output(temp_file_name);

            //MERGE
            bloomfilter* filter = new bloomfilter();


			std::stringstream entries;

            //make space for filter metadata
            entries << "4294967295" << "," << num_pages_x + num_pages_y << "\n";
            std::string file_out_meta = entries.str();
            output << file_out_meta;
            entries.str("");
            
            int max_entries = 4;
            int num_entries = 0;
            int new_file_len = 0;
            for (int i = 0; i < num_lines_x + num_lines_y; i++){

                if (next_x_empty == true) {
                    bool can_read = read_next(&f_x, &next_x_key, &next_x_val);
                    if (can_read == true) {
                        //std::cout << "NEXT X: " << next_x_key << std::endl;
                        next_x_empty = false;
                    }
                } 
                if (next_y_empty == true) {
                    bool can_read = read_next(&f_y, &next_y_key, &next_y_val);
                    if (can_read == true) {
                        //std::cout << "NEXT Y: " << next_y_key << std::endl;
                        next_y_empty = false;
                    }
                }

                int key; int val;
                bool stop = false;
                if (next_x_empty == true && next_y_empty == true) {
                    new_file_len--;
                    stop = true;
                } else if (next_x_empty == true) {
                    entries << next_y_key << "," << next_y_val << "\n";
                    next_y_empty = true;
                    //std::cout << "FLUSH MERGE NEXT: " << next_y_key << std::endl;
                } else if (next_y_empty == true) {
                    entries << next_x_key << "," << next_x_val << "\n";
                    next_x_empty = true;
                    //std::cout << "FLUSH MERGE NEXT: " << next_x_key << std::endl;
                } else if (next_x_key == next_y_key) {
                    //update or delete
                    entries << next_x_key << "," << next_x_val << "\n";
                    next_x_empty = true;
                    next_y_empty = true;
                    filter->insert(next_x_key);
                } else if (next_x_key < next_y_key) {
				    entries << next_x_key << "," << next_x_val << "\n";
                    next_x_empty = true;
                    filter->insert(next_x_key);

                    //std::cout << "FLUSH MERGE NEXT: " << next_x_key << std::endl;
                } else {
				    entries << next_y_key << "," << next_y_val << "\n";
                    next_y_empty = true;
                    filter->insert(next_y_key);

                    //std::cout << "FLUSH MERGE NEXT: " << next_y_key << std::endl;
                }
                new_file_len++;

                if (stop == true || num_entries >= max_entries || i == num_lines_x + num_lines_y - 1) {
                    std::string file_out = entries.str();
                    output << file_out;

                    entries.str("");
                }
            }
            
            //std::cout << "DONE. " << std::endl;

            output.clear();
            output.seekp(0);

            output << std::setfill('0') << std::setw(10) << filter->get_bitmap();

			output.close();

			f_x.close();
			f_y.close();

            //just merged two files; mark greater one deleted.
            file_bitmap[file_num_x] = false;
            file_bitmap[file_num_y] = true;
            //rename to the lower number
            remove(get_file_by_ind(file_num_x).c_str());
            remove(get_file_by_ind(file_num_y).c_str());
            //this->num_files--;
            rename(temp_file_name, get_file_by_ind(file_num_y).c_str());

            *new_len = new_file_len;
            
            delete filter;

            return true;
        }

        /**
         * merges two ssts
         *
         * @param file_num_x the newer page to be merged
         * @param file_num_y the older page to be merged
         * @param new_len sets as the new files length
         * @return true on success
         */
        void flush_merge_step(LSM_NODE* node){
            if (node->num_files == LSM_SIZE) {
                //std::cout << "row filled" << std::endl;
                //only supports LSM_SIZE == 2
                int file_num = node->page_origin[0];
                int new_len = 0;
                //newest files goes first
                merge_file(node->page_origin[1], node->page_origin[0], &new_len);
                node->clear();

                if (node->child == nullptr) {
                    node->child = new LSM_NODE();
                }

                node->child->add_file(file_num, new_len);
                flush_merge_step(node->child);
            } 

            return;
        }

        /**
         * for flushing a page from memtab
         *
         * @param list the array of pairs to save
         * @param len length of list
         * @return true on success
         */
        bool flush(kv_pair* list, int len){
            if (this->root == nullptr) {
                LSM_NODE* node = new LSM_NODE();
                this->root = node;
            }

            int file_num = num_files;
            bool result = write(get_next_file_name().c_str(), list, len);
            
            //std::cout << "flushing" << std::endl;
            this->root->add_file(file_num, 1);
            flush_merge_step(this->root);

            return true;
        }

        /**
         * for saving tree structure metadata
         *
         * @return true on success
         */
        bool save_tree(){
            std::ofstream output(get_lsmt_file_name(), std::ofstream::trunc);

            output << num_files << "\n" ;
            for (int i = 0; i < BITMAP_SIZE; i++) {
                output << file_bitmap[i] ;
                if (i < BITMAP_SIZE - 1) {
                    output << ",";
                }
            }
            output << "\n" ;

            LSM_NODE* curr = root;
            while (curr != nullptr) {
                
                int page_origin = -1;
                int page_length = -1;

                if (curr->num_files > 0) {
                    page_origin = curr->page_origin[0];
                    page_length = curr->page_length[0];
                }

                output << curr->num_files << "," << page_origin << "," << page_length << "\n" ;

                curr = curr->child;
            }

            output.close();
            clear();

            return true;
        }

        /**
         * for deleting the tree's saved metadata
         *
         * @return true on success
         */
        bool del_tree(){
            return remove((db_name + (std::string)(".lsmt")).c_str());
        }

        /**
         * for loading the tree's metadata
         *
         * @return true on success
         */
        bool load_tree(){
            
            std::ifstream f(get_lsmt_file_name());
            if(!f.is_open())
            {
                std::cout << "Failed to open lsmt" << std::endl;
                return false;
            }

            // int num_lines = std::count(std::istreambuf_iterator<char>(f), 
            //     std::istreambuf_iterator<char>(), '\n') - 1;
            // f.clear();
            // f.seekg(0);

            std::cout << "Building lsmt: " << std::endl;

            //get meta
            std::string num_files_str;
            std::getline(f, num_files_str);
            this->num_files = stoi(num_files_str);

            for (int i = 0; i < BITMAP_SIZE; i++) {
                std::string bitmap_val;
                if (i == BITMAP_SIZE - 1) {
                    std::getline(f, bitmap_val);
                } else {
                    std::getline(f, bitmap_val, ',');
                }
                this->file_bitmap[i] = stoi(bitmap_val);
            }

            std::string num_node_files, page_origin, page_length;
            //get rows
            LSM_NODE* prev = nullptr;
            while(std::getline(f, num_node_files, ','))
            {
                LSM_NODE* node = new LSM_NODE();
                std::getline(f, page_origin, ',');
                std::getline(f, page_length);

                if (this->root == nullptr) {
                    this->root = node;
                } else {
                    prev->child = node;
                }
                
                node->num_files = stoi(num_node_files);
                if (node->num_files > 0) {
                    node->page_origin[0] = stoi(page_origin);
                    node->page_length[0] = stoi(page_length);
                }
                
                prev = node;
            }

            
            return true;
        }

        lsmtree(const char* db_name){
            this->root = nullptr;
            this->db_name = db_name;
            this->num_files = 0;

            for (int i = 0; i < BITMAP_SIZE; i++) {
                this->file_bitmap[i] = false;
            }
        }


};