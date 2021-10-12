#include "../Headers/ex2.h"


List::List(){
	head = NULL;
	tail = NULL;
}

List::~List(){
	Node* temp = this->head;
	Node* temp1 = NULL;
	while (temp != NULL){
		temp1 = temp;
		temp = temp->next;
		//delete temp1->record;
		delete temp1;
	}

};

void List::delete_recs(){
	Node* temp = this->head;
	Node* temp1 = NULL;
	while (temp != NULL){
		temp1 = temp;
		temp = temp->next;
		delete temp1->record;
		//delete temp1;
	}
}

Node* List::gethead(){
	return this->head;
}

void List::insertNode(Record* record){
	Node* temp = new Node;
	temp->record = record;
	temp->next = NULL;

	if (head == NULL){
		head = temp;
		tail = temp;
	}
	else{
		tail->next = temp;
		tail = tail->next;
	}
}

void List::printList(){
	Node* temp = head;
	while (temp != NULL){

		cout << temp->record->recordID <<  " " << temp->record->status <<" " <<
		temp->record->firstName << " " << temp->record->lastName << " " << temp->record->disease 
		<< " " << temp->record->country << " " << temp->record->date << " " << temp->record->age << endl;

		temp = temp->next;
	}
}

bool List::canEnter(Record* record){
	string id = record->recordID;
	Node* temp = this->head;
	while (temp != NULL){
		if (temp->record->recordID == id){
			if (temp->record->status == "ENTER")
				return 1;
		}
		temp = temp->next;
	}
	return 0;
}

string List::searchPatientRecord(string recordID){
	Node* temp = this->head;
	string message="";
	int flag = 0;
	while (temp != NULL){
		if(temp->record->recordID == recordID){
			if (temp->record->status == "ENTER"){
				message = recordID + " " + temp->record->firstName + " " + temp->record->lastName + " " + temp->record->disease + 
				" " + temp->record->age + " " + temp->record->date;
			}
			if (temp->record->status == "EXIT"){
				flag = 1;
				message = message + " " + temp->record->date;
			}
		}
		temp = temp->next;
	}

	if (flag == 0 && message != "")
		message = message + " --";
	
	return message;
}