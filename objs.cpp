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