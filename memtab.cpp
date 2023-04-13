#include <iostream>
#include <algorithm>
#include <fstream> 
#include <sstream>
#include <string>
#include <random>
#include <cstring>
#include "objs.h"
/*
struct kv_pair
{//kv pair
	int key;
	int value;
	kv_pair(int a, int  b) : key(a), value(b){}
};

struct list_node
{//linked list node
	int key;
	int value;
	int length;
	list_node * next;


	list_node(int key, int value) : key(key), value(value), next(nullptr), length(1){}
};
*/
static void print_sst(SST* sst)
{
	std::cout << "-----------------------------------SST print start : " << sst->key << "  ----------------------------------------------" << std::endl;
	for(int i = 0; i < sst->entries; i++)
		std::cout << "Key: " << sst->data[i].key << " Value: " << sst->data[i].value <<  std::endl; 
}
struct Node 
{//binary tree node
	int key;
	int value;
	int height;
	Node* left;
	Node* right;

	Node(int key, int val) : key(key), value(val), height(1), left(nullptr), right(nullptr) {}
};

static void deleteTree(Node* n)
{/*
delete the three rooted at n
*/
	if(!n)
		return;
	deleteTree(n->right);
	deleteTree(n->left);
	delete n;
}

static void freeTree(Node* n)
{/*
free the memory used by a heapified tree at n
*/
	if(!n)
		return;
	freeTree(n->right);
	freeTree(n->left);
	free(n);
}

static void heapifyTree(Node* n)
{//save this tree in heap !!!!!!EXCEPT FOR THE ROOT(that is to be done by the calling function)
	if(!n)
		return;
	if(n->left)
	{
		Node* newLeft = (Node*)(malloc(sizeof(Node)));
		std::memcpy(newLeft, n->left, sizeof(Node));
		n->left = newLeft;
		heapifyTree(n->left);
	}
	if(n->right)
	{
		Node* newRight = (Node*)(malloc(sizeof(Node)));
		std::memcpy(newRight, n->right, sizeof(Node));
		n->right= newRight;
		heapifyTree(n->right);
	}
	
}

/*
static void deleteList(list_node* n)
{//free the linked list
	if(!n)
		return;
	deleteList(n->next);
	delete(n);
}
*/


class memtab
{//an augmented avl tree
	private:
	
		//class var
		int memtable_size;
		int entries;
		
		//functions 
		int get_height(Node * n)
		{
			return n ? n->height : 0;
		}
		
		int get_balance(Node * n)
		{
			return n ? get_height(n->left) - get_height(n->right) : 0;
		}
		
		void set_height(Node * n)
		{
			n->height = std::max(get_height(n->left), get_height(n->right)) + 1;
		}
	
		Node* leftRotate(Node * n)
		{
			Node* x = n->right;
			Node* y = x->left;
			x->left = n;
			n->right = y;
			set_height(n);
			set_height(x);
			return x;
		}

		Node* rightRotate(Node * n)
		{
			Node* x = n->left;
			Node* y = x->right;
			x->right = n;
			n->left = y;
			set_height(n);
			set_height(x);
			return x;
			}
			
		Node* balance(Node* n) 
		{
			set_height(n);
			//balance rightwards
			if (get_balance(n) > 1) 
			{
				if (get_balance(n->left) < 0) 
				{
					n->left = leftRotate(n->left);
				}
				return rightRotate(n);
			}
			//balance leftwards
			if (get_balance(n) < -1) 
			{
				if (get_balance(n->right) > 0) 
				{
					n->right = rightRotate(n->right);
				}
				return leftRotate(n);
			}
			return n;
		}
		
		Node* insert(Node* n, int key,int value) 
		{
			if (!n) { 
				return new Node(key, value);
			}

			if (key < n->key) {
				n->left = insert(n->left, key, value);
			} else if (key > n->key) {
				n->right = insert(n->right, key, value);
			} else {
				return n;
			}

			return balance(n);
		}
		
		void inOrderTraversal(Node* n) 
		{
			//print using inorder traversal
			if (n) 
			{
				inOrderTraversal(n->left);
				std::cout << n->key << " " << n ->value <<std::endl;
				inOrderTraversal(n->right);
			}
		}
		
		list_node* treeToBuffer(Node * n)
		{
			/*
			Returns a linked-list of integers representing this tree.
			*/
			list_node* leftLinked;
			list_node* rightLinked;
			list_node* curr;
			list_node* head = new list_node(n->key, n->value);
			curr = head;
			if(n->left)
			{
				curr = treeToBuffer(n->left);
				head = curr;
				while(curr->next)
				{
					curr = curr->next;
				}
				curr->next = new list_node(n->key, n->value);
				curr = curr->next;
				head->length += 1;
			}
			if(n->right)
			{
				rightLinked = treeToBuffer(n->right);
				curr->next = rightLinked;
				head->length += rightLinked->length;
			}				
			return head;
		}
	
		Node* binTreeSearch(int key, Node* n)
		{
			/*
			Find the value associated with key in this memtable (AVL tree)
			returns a pointer to the node if it exists
			returns a null pointer if the requested key is not in the table
			*/
			if(!n)
				return nullptr;
			//std::cout << n->key << "  " << n->value << std::endl;
			if(n->key == key)
				return n;
			
			if(n->key > key)
				return binTreeSearch(key, n->left);
			
			if(n->key < key)
				return binTreeSearch(key, n->right);
			
		}
	public:
		Node* root;
		unsigned long key;//used for hashing 
		bool isFull()
		{
			//std::cout << entries << "/" << memtable_size << std::endl;
			return !(entries < memtable_size);
		}
		
		int getSize()
		{
			return memtable_size;
		}
		
		int getEntries()
		{
			return entries;
		}
		memtab(int cap)
		{
			this->memtable_size = cap;
			this->entries = 0;
			this->root = nullptr;
			this->key = rand();
		}
		
		int insert(int key, int value)
		{
			/*
			returns 0 on success, 1 on failure. Will no allow insertion of k/v pairs
			beyond the memtable_size of this memtable. 
			*/
			if (this->entries >= this->memtable_size)//full!
			{
				std::cerr << "Warning: table full, insertion not completed" << std::endl;
				return 1;
			}
			
			root = insert(root, key, value);
			this->entries++;
			//std::cout << "Inserted: " << key << " " << value << std::endl; 
			return 0;
		}
		  
		void printInorder()
		{
			/*
			For debugging: print tree in order of keys
			*/
			inOrderTraversal(root);
			std::cout << std::endl;
		}
		
		void inOrderFlush(const char* filename)
		{
			/*
			Flush the contents of the tree to a file.
			Files have KV pairs on seperate lines, delimited by a comma 
			eg:
			key1,value1
			key2,value2
			...
			*/
			list_node* a = treeToBuffer(root);
			std::cout << "flush start, total length: " << a->length << std::endl;
			
			int buf_len = a->length*2;
			int* buf = (int*)malloc(buf_len * sizeof(int));
			int count = 0;
			while(a)//populate the buffer with data
			{
				buf[count*2] = a->key;
				buf[count*2+1] = a->value;
				std::string bleh = "false";
				//if (a->next)
				//	bleh = "true";
				//std::cout << a->key << "     " << bleh << std::endl;//use for testing
				a = a->next;
				count++;
			}
	
			std::stringstream entries;
			for(int i = 0; i < count; i++)
			{
				entries << buf[i*2] << "," << buf[i*2+1] << "\n";
			}
			std::string file_out = entries.str();
			std::ofstream output((std::string)filename);
			output << file_out;
			output.close();
					
			return;
		}
		
		bool get(int key, int* result)
		{/*
		returns true on success, false on failure
		stores the resulting value pair in the int addr provided
		*/
			Node* val = binTreeSearch(key, root);
			if(val)
			{
				*result = val->value;
				return true;
			}	
			*result = -1;
			return false;
		}
		
		
		bool scan(int key1, int key2, list_node** result)
		{/*
		returns linked list of key, value pairs within range on success
		*/
			int start = key1;
			int end = key2;
			if(key1 > key2)
			{
				start = key2;
				end = key1;
			}
			
			list_node* head = treeToBuffer(root);		
			
			while(head && head-> key < start)
			{
				head = head->next;
			}
			
			if(!head)//failed to find a key in range
				return false;
			list_node* tail = head;
			int count = 1;
			while(tail->next && (tail->next)-> key < end)
			{
				count++;
				tail = tail->next;
			}
			tail->next = nullptr;
			head->length = count;
			
			*result = head;
			list_node* traverse = head;
			
			return true;
		}
		
		void deleteAll()
		{
			deleteTree(root);
			root = nullptr;
			entries = 0;
		}
		
};


static memtab* build_from_file(const char* filename) 
{//###note to self### need to set some global var for memory cap on memtable!

//Builds a memtable from filename and returns a pointer to the created memtable in memory.
//returns nullptr if it fails. 

	using namespace std;
	memtab* table = new memtab(10);
	
	std::ifstream f(filename);
	if(!f.is_open())
	{
		std:cout << "Failed to open file: " << filename << std::endl;
		return nullptr;
	}
	std::string key, val;
	
	std::cout << "Begin build "<< filename << std::endl;
	while(std::getline(f, key, ','))
	{
		//std::cout << "key: " << key;
		std::getline(f, val);
		//std::cout << "Val: " << val << std::endl; 
		
		table->insert(stoi(key), stoi(val));
	}
	return table;
	
};
static const int SST_BYTES = 4096 - sizeof(SST) - 30; //just shy of 4kb per sst including metadata
static const int MAX_ENTRIES = 2* (SST_BYTES/2) / sizeof(kv_pair);//make this number always even for convenience. 

struct SST_node
{
	SST* sst;
	int min;
	int max;
	unsigned long sst_key;
	int split;
	SST_node* left;
	SST_node* right;
	SST_node(): sst(nullptr), left(nullptr), right(nullptr), split(0), sst_key(0), min(0), max(0) {}	
};


class SST_directory
{
	SST_node* root;
	private:
		void sstInsert(SST* sst, kv_pair* kv)
		{//use binary search, insert the kv into the sst at the appropriate index in the SST's data field.
			int top = sst->entries;//one end of the sst.
			int bottom = 0;
			while(top-bottom > 1)//converge the range in binary search
			{
				int mid = (top + bottom)/2;
				if(sst->data[mid].key > kv->key)
				{
					top = mid;
				}
				else
				{
					bottom = mid;
				}
			}//top and bottom now straddle the correct key's index, if neither are the key themselves.
		
			if(sst->data[top].key == kv->key)//can treat this as update if key already in SST, although this is unintended, print warning
			{
				std::cout << "WARNING: inserted KV pair that was already in SST! Handling as update instead. key: " << kv->key << " SST: " << sst->key << std::endl;
				sst->data[top].value = kv->value;
				return;
			}
			if(sst->data[bottom].key == kv->key)
			{
				std::cout << "WARNING: inserted KV pair that was already in SST! Handling as update instead. key: " << kv->key << " SST: " << sst->key << std::endl;
				sst->data[bottom].value = kv->value;
				return;
			}
			
			//make the update to the sst.
			if(sst->entries > MAX_ENTRIES)
			{
				std::cerr << "Error, attempting to insert into full SST, insertion incomplete" << std::endl;
				return;
			}
			
			kv_pair* temp = (kv_pair*)(malloc(MAX_ENTRIES * sizeof(kv_pair)));
			std::memcpy(temp, sst->data, MAX_ENTRIES*sizeof(kv_pair));
			std::memcpy(&(sst->data[top+1]), &(temp[top]), MAX_ENTRIES - top - (int)(sizeof(kv_pair)));
			sst->data[top].key = kv->key;
			sst->data[top].value = kv->value;
			free(temp);
			sst->entries++;
			return;
		}
	
	public: 
		SST_directory()
		{
			root = nullptr;
		}
		unsigned long get(int key, bool* loaded, bool* found)
		{//return the sst key for the given key. loaded is set to true when the target SST is in the buffer, false otherwise
		//if found flag is set to true, otherwise false;
			*found = false;
			SST_node* curr = root;
			*loaded  = false;
			while(true)
			{
				//leaf node
				if(!curr->left)
				{
					if(key >= curr->min && key <= curr->max)
					{
						if(curr->sst)
							*loaded = true;
						*found = true;
						return curr->sst_key;
					}
					else
					{
						std::cerr << "WARNING: tried to get key not in Database!" << std::endl;
						return 0;//Indicates that we could not find the sst. 
					}
				}
				else
				{
					if(key >= curr->split)
						curr = curr->right;
					else
						curr = curr->left;
				}
			}
			return 0;//for the compiler
		}
		
		
		SST* insert(kv_pair* kv, SST_node* n)
		{//will return nullptr if an existing SST was updated and no new tables made.
		//if the insertion caused a split, will return an SST of the *NEWLY created table that DOESNT contain the 
		//just inserted kv_pair. the existing SST will contain it, and the new one will have to be handled.
			SST_node* curr = root;
			
			if(!root)//if this is the first ever insert, make a root sst_node
			{
				root = new SST_node();
				root->sst = new SST();
				root->sst_key = root->sst->key;
				root->sst->data = (kv_pair*)(malloc(MAX_ENTRIES * sizeof(kv_pair)));
				root->sst->data[root->sst->entries].key = kv->key;
				root->sst->data[root->sst->entries].value = kv->value;
				root->sst->entries++;
				root->sst->minkey = kv->value;
				root->sst->maxkey = kv->value;
				return root->sst;
			}
			
			if(!n->sst)//internal node
			{
				if(!n->left)//not internal node... screwed up
				{
					std::cerr << "CRITICAL ERROR: trying to get/insert into non-loaded SST" << std::endl;
					return nullptr;
				}
				if(kv->key >= n->split)//internal nodes are gauranteed to have both left and right child 
					return insert(kv, n->right); 
				else
					return insert(kv, n->left);
			}
			
			//leaf node, we insert into this SST
			if(n->sst->entries < MAX_ENTRIES)
			{
				int i = n->sst->entries;
				n->sst->data[i].key = kv->key;
				n->sst->data[i].value = kv->value;
				n->sst->entries++;
				//update range of table if needed
				if(kv->key > n->sst->maxkey)
					n->sst->maxkey = kv->key;
				if(kv->key < n->sst->minkey)
					n->sst->minkey = kv->key;
				return nullptr;
			}
			//full, require to split the tree
			int mid_key = n->sst->data[MAX_ENTRIES/2].key;
			kv_pair* temp = (kv_pair*)(malloc(MAX_ENTRIES * sizeof(kv_pair)));
			std::memcpy(temp, n->sst->data, MAX_ENTRIES*sizeof(kv_pair));
			
			//this will now become an internal node:
			n->split = mid_key;
			n->left = new SST_node(); 
			n->right = new SST_node();
			
			if(kv->key >= mid_key)//ends up on right side of split
			{
				n->right->sst = n->sst;
				n->right->sst->minkey = mid_key;
				std::memcpy(n->right->sst->data, temp + (MAX_ENTRIES/2)*sizeof(kv_pair), (MAX_ENTRIES/2)*sizeof(kv_pair));
				n->right->sst->entries = MAX_ENTRIES/2;
				sstInsert(n->right->sst, kv);
				
				
				n->left = new SST_node();
				n->left->sst = new SST();
				n->left->sst_key = n->left->sst->key;
				n->left->sst->maxkey = mid_key - 1;
				n->left->sst->minkey = temp[0].key;
				n->left->sst->entries = MAX_ENTRIES/2;
				n->left->sst->data = (kv_pair*)(malloc(MAX_ENTRIES * sizeof(kv_pair)));
				std::memcpy(n->left->sst->data, temp, (MAX_ENTRIES/2)*sizeof(kv_pair));
				
				
			}
			else
			{
				n->left->sst = n->sst;
				n->left->sst->maxkey = mid_key-1;
				n->left->sst->entries = MAX_ENTRIES/2;
				std::memcpy(n->left->sst->data, temp , (MAX_ENTRIES/2)*sizeof(kv_pair));
				sstInsert(n->left->sst, kv);
				
				n->right = new SST_node();
				n->right->sst = new SST();
				n->right->sst_key = n->right->sst->key;
				n->right->sst->minkey = mid_key;
				n->right->sst->maxkey = temp[MAX_ENTRIES-1].key;
				n->right->sst->entries = MAX_ENTRIES/2;
				n->right->sst->data = (kv_pair*)(malloc(MAX_ENTRIES * sizeof(kv_pair)));
				std::memcpy(n->right->sst->data, temp + (MAX_ENTRIES/2)*sizeof(kv_pair), (MAX_ENTRIES/2)*sizeof(kv_pair));
			}
			free(temp);
			n->sst = nullptr;
			
			if(kv->key >= mid_key)
				return n->left->sst;
			else
				return n->right->sst;
	}
	
	void testInsert()
	{

		SST* test = new SST();
		test->data = (kv_pair*)(malloc(200 * sizeof(kv_pair)));
		
		kv_pair* kv1 = new kv_pair(0, 0);
		kv_pair* kv2 = new kv_pair(1 ,1);
		kv_pair* kv3 = new kv_pair(2, 2);
		

		
		sstInsert(test, kv1);
		print_sst(test);
		
		sstInsert(test, kv2);
		print_sst(test);
		
		sstInsert(test, kv3);
		print_sst(test);

		for(int i = 3; i < 100; i++)
		{
			if(i != 50)
			{
				kv_pair* k;
				k->key = i;
				k->value = i;
				sstInsert(test, k);
			}
		}
		
		kv_pair* k4 = new kv_pair(50, 50);
		kv_pair* k5 = new kv_pair(99, 1000);

		sstInsert(test, k4);
		print_sst(test);
		
		sstInsert(test, k5);
		print_sst(test);	
	}
};

//for testing the tree
/*
int main() {
    memtab tab0(6);
    tab0.insert(1, 881);
    tab0.insert(4, 882);
    tab0.insert(2, 883);
    tab0.insert(3, 884);
    tab0.insert(5, 885);
    tab0.insert(6, 886);

    std::cout << "In order traversal of key/value:" << std::endl;
    tab0.printInorder();
	//tab0.inOrderFlush("yay.txt");
	
	
	std::cout << "Search for key 5:" << std::endl;
	bool a = false;
	int b;
	a = tab0.get(3, &b);
	std::cout << a << std::endl;
	std::cout << b << std::endl;
	
	std::cout << "scan start" << std::endl;
	list_node* s;
	if(tab0.scan(2,4, &s))
	{
		while(s)
		{
			std::cout << s->key << "," << s->value << "|";
			s = s->next;
		}
		std::cout<<std::endl;
	}
	memtab* built = build_from_file("yay.txt");
	built->printInorder();
	built->deleteAll();
	(*built).printInorder();
	
	
	
    return 0;
}
*/