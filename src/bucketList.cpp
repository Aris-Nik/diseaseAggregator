#include "../Headers/ex2.h"

bucketList::bucketList(int capacity,string country){
	this->capacity = capacity;
	this->currentRecords = 0;
	this->country = country;
	this->records = new bucketNode[capacity];
	for (int i = 0; i < capacity; i++){
		this->records[i].disease = "";
		this->records[i].list = NULL;
	}

}

bucketList::~bucketList(){
	for (int i = 0;i< this->currentRecords; i++){
		delete this->records[i].list;
	}
	delete[] this->records;
};


void bucketList::insert(Record* rec){
	int index = isInside(rec->disease);
	if (index == -1){
		this->records[this->currentRecords].disease = rec->disease;
		this->records[this->currentRecords].list = new List();
		this->records[this->currentRecords].list->insertNode(rec);
		this->currentRecords++;
	}
	else{																	//else disease is already on the "bucket" so we just add the record to the list of disease
		this->records[index].list->insertNode(rec);
	}
}

int bucketList::isInside(string temp){
	for (int i=0; i < this->currentRecords; i++){
		if (temp == this->records[i].disease)
			return i;
	}
	return -1;
}

void bucketList::print_diseases(){
	for (int i = 0; i < this->currentRecords; i++){
		cout << "~~~~~~~~~~~~~~ FOR DISEASE:  "  << this->records[i].disease << endl;
		this->records[i].list->printList();
	}
}

string bucketList::age_range(string date){

	string message = "";
	for (int i = 0;i<this->currentRecords; i++){
		int arr[4] ={0,0,0,0};							//4 age ranges
		
		Node* temp = this->records[i].list->gethead();
		while (temp != NULL){
			if (temp->record->date == date && temp->record->status == "ENTER"){
				int age = stoi(temp->record->age);
				if (age >= 0 && age <= 20)
					arr[0]++;
				if (age >= 21 && age <= 40)
					arr[1]++;
				if (age >= 41 && age <= 60)
					arr[2]++;
				if (age >=60)
					arr[3]++;
			}
			temp = temp->next;
		}
		message = message +  date + "\n" + this->country + "\n" + this->records[i].disease + "\n" + "Age range 0-20 years : " + to_string(arr[0]) + " cases\n" + "Age range 21-40 years : " +
		to_string(arr[1]) + " cases\n" + "Age range 41-60 years : " + to_string(arr[2]) + " cases\n" + "Age range 60+ years : " + to_string(arr[3]) + " cases \n\n";
		//cout << message;
		//cout << date << endl << this->country << endl << this->records[i].disease << endl <<
		//"Age range 0-20 years : " << arr[0] << " cases" << endl << 
		//"Age range 21-40 years : " << arr[1] << " cases" << endl << 
		//"Age range 41-60 years : " << arr[2] << " cases" << endl << 
		//"Age range 60+ years : " << arr[3] << " cases" << endl;
	}
	return message;
}

string bucketList::topk_AgeRanges(string k,string virusName,string date1,string date2){
	string message = "";
	long int total_date1 = totalDays(date1);
	long int total_date2 = totalDays(date2);
	int arr[4] = {0,0,0,0};
	for (int i = 0; i < this->currentRecords; i++){
		
		if (this->records[i].disease == virusName){

			Node* temp = this->records[i].list->gethead();

			while(temp != NULL){
				if (temp->record->status == "ENTER"){
					long int total_days = totalDays(temp->record->date);
					if (total_date1 <= total_days && total_days >= total_date1){
						int age = stoi(temp->record->age);
						if (age >= 0 && age <= 20)
							arr[0]++;
						if (age >= 21 && age <= 40)
							arr[1]++;
						if (age >= 41 && age <= 60)
							arr[2]++;
						if (age >=60)
							arr[3]++;
					}
				}
				temp = temp->next;
			}
		}
	}
	int real_k = atoi(k.c_str());
	if (real_k > 4)
		real_k = 4;
	int total = arr[0] + arr[1] + arr[2] + arr[3];
	for (int temp_k = 0; temp_k < real_k; temp_k++){
		int max = -1;
		int maxi = -1;
		for (int j = 0; j < 4; j++){
			if (arr[j] > max){
				max = arr[j];
				maxi = j;
			}
		}
		arr[maxi] = -1;
		int perc = (max * 100) / total;
		if(maxi == 0){
			message = message + "0-20: " + to_string(perc) + "%\n";
		}
		if (maxi == 1){
			message = message + "21-40: " + to_string(perc) + "%\n";
		}
		if (maxi == 2){
			message = message + "41-60: " + to_string(perc) + "%\n";
		}
		if (maxi == 3){
			message = message + "60+: " + to_string(perc) + "%\n";
		}
	}
	return message;
}

int bucketList::diseaseFrequency(string date1,string date2,string virusName){
	int sum = 0;
	long int total_date1 = totalDays(date1);
	long int total_date2 = totalDays(date2);
	for (int i = 0; i < this->currentRecords; i++){
		if (this->records[i].disease == virusName){

			Node* temp = this->records[i].list->gethead();

			while (temp != NULL){
				if (temp->record->status == "ENTER"){
					
					long int total_days = totalDays(temp->record->date);
					if ( total_date1 <= total_days && total_date2 >= total_days )
						sum++;
				}
				temp = temp->next;
			}
			return sum;
		}
	}
	return sum;
}

int bucketList::numPatientDischarges(string date1,string date2,string virusName){
	int sum = 0;
	long int total_date1 = totalDays(date1);
	long int total_date2 = totalDays(date2);
	for (int i = 0; i < this->currentRecords; i++){
		if (this->records[i].disease == virusName){

			Node* temp = this->records[i].list->gethead();

			while (temp != NULL){
				if (temp->record->status == "EXIT"){
					long int total_days = totalDays(temp->record->date);
					if ( total_date1 <= total_days && total_date2 >= total_days )
						sum++;
				}
				temp = temp->next;
			}
			return sum;
		}
	}
	return sum;
}


