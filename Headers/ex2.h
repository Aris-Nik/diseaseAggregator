#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>
#include <errno.h>
#include <sstream>
#include <sys/types.h>
#include <stdio.h>
#include <unistd.h>
#include <dirent.h>
#include <cstring>
#include <sys/wait.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <csignal>


using namespace std;



class files_list{
public:
	string name;
	files_list* next;

};

typedef struct Record{
	string recordID;
	string status;
	string firstName;
	string lastName;
	string disease;
	string country;
	string date;
	string age;
}Record;

typedef struct Node{
	Record* record;
	struct Node* next;
}Node;

class List{

	private:
		Node* head;
		Node* tail;

	public:
		List();
		~List();
		void insertNode(Record* record);
		void printList();
		bool canEnter(Record* record);
		string searchPatientRecord(string recordID);
		Node* gethead();
		void delete_recs();
		
};

typedef struct bucketNode{
	string disease;
	List* list;
}bucketNode;

class bucketList{

	private:
		bucketNode* records;
		int capacity;
		int currentRecords;
		string country;

	public:
		bucketList(int capacity,string country);
		~bucketList();
		void insert(Record* rec);
		int isInside(string name);
		void print_diseases();
		string age_range(string date);
		string topk_AgeRanges(string k,string virusName,string date1,string date2);
		int diseaseFrequency(string date1,string date2,string virusName);
		int numPatientDischarges(string date1,string date2,string virusName);


};



void signalHandler_worker(int signum);
void signalHandler( int signum );
void new_worker(pid_t p);
void child_death(int signum );
int worker(int index,string PtW,string WtP,int bufferSize,char* input_dir);
long int totalDays(string date);
bool isFileInside(int country,string name);
void sigurs1_fun();
