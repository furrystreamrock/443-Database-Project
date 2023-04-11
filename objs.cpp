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

static void deleteList(list_node* n)
{/*free the linked list*/
	if(!n)
		return;
	deleteList(n->next);
	delete(n);
}

struct SST
{
	kv_pair* data;//(kv_pair*)(malloc(SST_BYTES))
	int minkey;
	int maxkey;
	int entries; //must be < MAX_ENTRIES;
	unsigned short key;//hashable key
	SST(): data(nullptr), minkey(0), maxkey(0), key(0) {}
};

struct bucket_node
{//Can be set up for either storing AVL trees or an array of KV's + metadata depending on code version
	bucket_node * prev;
	bucket_node * next;
	//memtab * table;	 //archiac
	
	SST* sst;
	
	bool clockbit;
	bucket_node(): prev(nullptr), next(nullptr), sst(nullptr), clockbit(false){}
};

struct node_dll
{
	node_dll* next;
	node_dll* prev;
	bucket_node* target;
	node_dll(): next(nullptr), prev(nullptr), target(nullptr){}
};

