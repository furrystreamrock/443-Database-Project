#include <cstdint>
#include <math.h> 

const int HASH_COUNT = 5; //optimal is: (BIT_SIZE*32 / PAGE_LENGTH) * log(2)
const double TARG_FPR = 0.01;
const int BIT_SIZE = 1; //optimal is: ceil( ( (PAGE_LENGTH * abs(log(TARG_FPR))) / (pow(log(2), 2)) ) / 32 )


class bloomfilter {
    int count;
    //uint32_t bitmap[BIT_SIZE];
    uint32_t bitmap;
    
    private:

        uint32_t hash_x(int hash_num, int input){
            int val = 2 + hash_num;
            return input % ((val) + (val * (val + 1) / 2));
        }

        uint32_t hash(int input){
            uint32_t result = 0;
            for (int i = 0; i < HASH_COUNT; i++) {
                result = result | hash_x(i, input);
            }

            return result;
        }

    public:
        /**
         * gets the bloom filter's bitmap
         *
         * @return bitmap
         */
        uint32_t get_bitmap(){
            return this->bitmap;
        }

        /**
         * sets the bloom filter's bitmap
         *
         * @param bitmap new bitmap
         */
        void set_bitmap(uint32_t bitmap){
            this->bitmap = bitmap;
        }
    
        /**
         * check to see if a key may be in the page associated with the filter
         *
         * @param key the key to check for possibility of existence
         * @return true if possible
         */
        bool check(int key){
            uint32_t bits = hash(key);
            if (bits & this->bitmap != bits) {
                //negative
                return false;
            } else {
                return true;
            }

        }

        /**
         * insert a new key in the filter
         *
         * @param key the key to insert and flip bits for
         */
        void insert(int key){
            this->bitmap = this->bitmap | hash(key);
            this->count++;
        }

        bloomfilter(){
            count = 0;
            bitmap = 0;
        }
};