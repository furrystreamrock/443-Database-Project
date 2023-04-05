#include <iostream>
#include <algorithm>
#include <fstream> 
#include <sstream>
#include <string>
#include <iostream>

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

struct Node 
{//binary tree node
	int key;
	int value;
	int height;
	Node* left;
	Node* right;

	Node(int key, int val) : key(key), value(val), height(1), left(nullptr), right(nullptr) {}
};




class memtab
{//an augmented avl tree
	private:
	
		//class var
		Node* root;
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
			KV are kept sequentially, every other index is a KV pair.
			E.g : K1|V1|K2|V2 ...
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
	
		
	public:
		memtab(int cap)
		{
			this->memtable_size = cap;
			this->entries = 0;
			this->root = nullptr;
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
			std::cout << "Inserted: " << key << " " << value << std::endl; 
			return 0;
		}
		  
		void print_inorder()
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
			
			int buf[a->length*2];
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
			for(int i = 0; i < (sizeof(buf)/sizeof(int))/2; i++)
			{
			
				entries << buf[i*2] << "," << buf[i*2+1] << "\n";
			
			}
			std::string file_out = entries.str();
			std::ofstream output(filename);
			output << file_out;
			output.close();
					
			return;
		}
		
};


static memtab* build_from_file(const char* filename) 
{//###note to self### need to set some global var for memory cap on memtable!
/*
Builds a memtable from filename and returns a pointer to the created memtable in memory.
returns nullptr if it fails. 
*/
	using namespace std;
	memtab table(10);
	
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
		
		table.insert(stoi(key), stoi(val));
	}
	return &table;
};


//for testing the tree
int main() {
    memtab tab0(6);
    tab0.insert(1, 881);
    tab0.insert(4, 882);
    tab0.insert(2, 883);
    tab0.insert(3, 884);
    tab0.insert(5, 885);
    tab0.insert(6, 886);

    std::cout << "In order traversal of key/value:" << std::endl;
    tab0.print_inorder();
	tab0.inOrderFlush("yay.txt");
	build_from_file("yay.txt");
    return 0;
}