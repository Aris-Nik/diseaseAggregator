#include "../Headers/ex2.h"



int count_of_countries = 0;
string* countries;
List** records_list;
bucketList** buckets;
string global_input_dir;
string** files;
int* count_of_files_array;
int fd1_worker;
int fd2_worker;
int global_buffer_size;

volatile sig_atomic_t sig_int = 0;
volatile sig_atomic_t sig_quit = 0;
volatile sig_atomic_t sig_usr1 = 0;

void signalHandler_worker( int signum ) {
    if (signum == SIGINT || signum == SIGQUIT){
       
        close(fd1_worker);
        close(fd2_worker);
        sig_quit = 1;
        
    } 
    else if (signum == SIGUSR1){                                       
        sig_usr1 = 1; 
    }
}

void sigurs1_fun(){
    
    for (int i = 0; i < count_of_countries; i++){
        string sub_dir = global_input_dir + "/" + countries[i];
        struct dirent *entry;
        DIR *dir;
        dir = opendir(sub_dir.c_str());
        while ((entry = readdir(dir)) != NULL){
            if (entry->d_name[0] != '.'){
                if (!isFileInside(i,entry->d_name)){                                         //here is a new file add it to the files and resize the array because we added one more file
                    
                    string* temp = new string[count_of_files_array[i] + 1];
                    for (int c = 0; c < count_of_files_array[i]; c++)
                        temp[c] = files[i][c];
                    temp[count_of_files_array[i]] = entry->d_name;
                    delete[] files[i];
                    files[i] = temp;
                    count_of_files_array[i]++;
                    string file_txt = global_input_dir + "/" + countries[i] + "/" + entry->d_name;
                    char file_c[100];
                    strcpy(file_c,file_txt.c_str());
                    ifstream textfile(file_c);
                    
                    if (textfile.is_open() && textfile.good()){
                        
                        string line = "";
                        while( getline(textfile, line)){   
                            Record* rec = new Record();
                            istringstream ss(line);
                            string word;
                            ss >> word;
                            rec->recordID = word;
                            ss >> word;
                            rec->status = word;
                            ss >> word;
                            rec->firstName = word;
                            ss >> word;
                            rec->lastName = word;
                            ss >> word;
                            rec->disease = word;
                            ss >> word;
                            rec->age = word;
                            rec->country = countries[i];
                            
                            string temp100 = files[i][count_of_files_array[i] - 1].substr(0, 10);
                            rec->date = temp100;
                            
                            if (rec->status == "EXIT"){
                                if (records_list[i]->canEnter(rec)){
                                    records_list[i]->insertNode(rec);
                                    buckets[i]->insert(rec);
                                }
                                else{
                                    fprintf(stderr, "ERROR\n");        
                                    delete rec;
                                }
                            }else{
                                
                                records_list[i]->insertNode(rec);
                                
                                buckets[i]->insert(rec);
                            }   
                        }
                    }
                    textfile.close();
                    string age_range = buckets[i]->age_range(files[i][count_of_files_array[i] - 1].substr(0,10));
                    cout << age_range << endl;
                }
            }
        }
    }
}


bool isFileInside(int country,string name){
    for (int i = 0; i < count_of_files_array[country]; i++)
        if (name == files[country][i])
            return 1;
    return 0;
}


int worker(int index,string PtW,string WtP,int bufferSize,char* input_dir){
	
    signal(SIGINT,signalHandler_worker);
    signal(SIGQUIT,signalHandler_worker);
    signal(SIGUSR1,signalHandler_worker);
	int fd;
    global_buffer_size = bufferSize;
	int num;
     
    global_input_dir = input_dir;
   

    long int success = 0;
    long int fail = 0;
   
    string dolla = "$";


    if ((fd1_worker = open(PtW.c_str(), O_RDONLY)) < 0)
    	perror("child - open");
                                                                                                               
    if ((fd2_worker = open(WtP.c_str(), O_WRONLY)) < 0)
        perror("worker - open");
    int i = 0;
   
    int flag=0;

    char out[bufferSize + 1];
    
    char buf[bufferSize + 1];                                                                   //need one more byte for terminal char '\0'
    memset(buf,0,bufferSize);                                                                           //initialize buffer because valgrind complains
    string message="";
    string temp123;
    while ((num = read(fd1_worker,out,bufferSize)) > 0){
        out[num] = '\0';
        temp123 = out;
        if (temp123 == "$")
            break;
        message = message + out; 

    }
 
    int a = 0;
    string word1 = "";
    istringstream ss1(message);
    
    while (ss1 >> word1){
        count_of_countries++;                                                           //count the countries parent gave to this specific worker
    }
    countries = new string[count_of_countries];
    int i_temp = 0;
    word1 = "";
    istringstream ss2(message);
    while (ss2 >> word1){
        countries[i_temp++] = word1;                                                                //create an array of countries a worker has to handle
    }
    
    struct dirent *entry;
    DIR *dir;

    

    
    char dir_name[40];
    records_list = new List*[count_of_countries];                                              //for every country a worker has we have a list that we keep all patients
    buckets = new bucketList*[count_of_countries];                                          //for every country a worker has we have a list of diseases that contain a list of patients with that disease
   
    count_of_files_array = new int[count_of_countries];
    files = new string*[count_of_countries];

   
    for (int country_index = 0; country_index < count_of_countries; country_index++){
        struct dirent *entry;
        
        DIR *dir;
        sprintf(dir_name,"%s/%s",input_dir,countries[country_index].c_str());
        dir = opendir(dir_name);
        if (!dir)
            cout << "Directory not found with name " << input_dir << "/" << countries[country_index] << endl;
        int count_of_files=0;
        while ((entry = readdir(dir)) != NULL){
            if (entry->d_name[0] != '.'){        
               
                count_of_files++;
            }
        }
        closedir(dir);
        files[country_index] = new string[count_of_files];
        count_of_files_array[country_index] = count_of_files;
        dir = opendir(dir_name);
        int c = 0;
        while ((entry = readdir(dir)) != NULL){
            if (entry->d_name[0] != '.'){        
                files[country_index][c++] = entry->d_name;                                                                 //insert files (DD-MM-YYYY) on a array to sort them
            }
        }

        for (c = 0; c < count_of_files;c++){                                                            //order them by chronological order
            int minValue = totalDays(files[country_index][c]);
            int minj = c;
            for (int j = c + 1; j < count_of_files; j++ ){
                if (totalDays(files[country_index][j]) < minValue){
                    minValue = totalDays(files[country_index][j]);
                    minj = j;
                }
            }
            string temp = files[country_index][c];
            files[country_index][c] = files[country_index][minj];
            files[country_index][minj] = temp;
            
        }

        records_list[country_index] = new List();
        buckets[country_index] = new bucketList(100,countries[country_index]);                                                   //100 capacity
        

        closedir(dir);
        for (c = 0; c < count_of_files; c++){
            string age_range="";
            char* file_txt = new char[100];
            sprintf(file_txt,"%s/%s/%s",input_dir,countries[country_index].c_str(),files[country_index][c].c_str());
        
            ifstream textfile(file_txt);
            delete[] file_txt;
           
            if (textfile.is_open() && textfile.good()){

                string line = "";

                while( getline(textfile, line)){   
                    Record* rec = new Record();
                    istringstream ss(line);
                    string word;
                        ss >> word;
                        rec->recordID = word;
                        ss >> word;
                        rec->status = word;
                        ss >> word;
                        rec->firstName = word;
                        ss >> word;
                        rec->lastName = word;
                        ss >> word;
                        rec->disease = word;
                        ss >> word;
                        rec->age = word;
                        rec->country = countries[country_index];
                        
                        string temp100 = files[country_index][c].substr(0, 10);
                        rec->date = temp100;
                        if (rec->status == "EXIT"){

                            if (records_list[country_index]->canEnter(rec)){
                                records_list[country_index]->insertNode(rec);
                                buckets[country_index]->insert(rec);
                            }
                            else{
                                fprintf(stderr, "ERROR\n");
                                
                                delete rec;
                            }
                        }else{

                            records_list[country_index]->insertNode(rec);

                            buckets[country_index]->insert(rec);
                        }

                            

                }
                textfile.close();
            }
            else{
                cout << "Failed to open file" << endl;
            }

            


            
            age_range =  buckets[country_index]->age_range(files[country_index][c].substr(0,10));
        
            
            int message_length = age_range.length();
            int times = message_length/bufferSize;
            if (message_length % bufferSize != 0 || times == 0)
                    times++;
            string temp;
            for (int j = 0;j<times;j++){
                if (bufferSize < age_range.length()){
                    temp = age_range.substr(0,bufferSize);
                    age_range = age_range.substr(bufferSize,age_range.length() - bufferSize);
                }
                else{
                    temp = age_range.substr(0,age_range.length());
                    age_range = "";
                }
                strcpy(buf,temp.c_str());
                
                write(fd2_worker,buf,bufferSize);
            }

            
        }

    }
    

    strcpy(buf,dolla.c_str());
    write(fd2_worker,buf,1);                                              // charcater $ means worker is ready to work for the parent
    message ="";
    
    string answer;
    while(1){
        message = "";
        
        while ((num = read(fd1_worker,out,bufferSize)) > 0){
            out[num] = '\0';
            string temp123 = out;
            if (temp123 == "$")
                break;
            message = message + out; 
            
            
        }
        if (sig_quit == 1){
            
          
            sig_int = 1;
            ofstream file;
            string f_name = "log_file." + to_string(getpid());
            file.open(f_name);
            for (int i = 0; i < count_of_countries; i++){
                file << countries[i];
                file << "\n";
            }
            file << "TOTAL ";
            file << to_string(success + fail);
            file << "\n SUCCESS ";
            file << to_string(success);
            file << "\n FAIL ";
            file << to_string(fail);
            file << "\n";
            file.close();
           
            for (int i = 0; i < count_of_countries; i++){ 
                records_list[i]->delete_recs();
                delete records_list[i];
            }
            delete[] records_list;
            for (int i =0; i < count_of_countries; i++)
                delete buckets[i];
            delete[] buckets;
            delete[] count_of_files_array;
            for (int i = 0; i < count_of_countries; i++)
                delete[] files[i];                                            //for every country a worker has we have a list that we keep all patients
            delete[] files;

            
            delete[] countries;
            
            break;
            
        }


        answer = message;
        
        istringstream ss(answer);
        string answer1;
        ss >> answer1;
        if (answer1 == "/diseaseFrequency"){
            string virusName;
            string date1;
            string date2;
            string country;
            ss >> virusName;
            ss >> date1;
            ss >> date2;
            ss >> country;

            if (virusName == "" || date1 == "" || date2 == ""){
                fail++;
                strcpy(buf,dolla.c_str());
                write(fd2_worker,buf,1);
            }
            else{
                success++;
                int sum=0;
                if (country == ""){
                    for (int i = 0;i < count_of_countries; i++){
                        sum = sum + buckets[i]->diseaseFrequency(date1,date2,virusName);
                    }
                }
                else{
                    for (int i = 0; i < count_of_countries; i++){
                        if (country == countries[i]){
                            
                            sum = buckets[i]->diseaseFrequency(date1,date2,virusName);
                            break;
                        }
                    }
                }
               
                string send = to_string(sum);
                int message_length = send.length();
                int times = message_length/bufferSize;
                if (message_length % bufferSize != 0 || times == 0)
                        times++;
                string temp;
                for (int j = 0;j<times;j++){
                    if (bufferSize < send.length()){
                        temp = send.substr(0,bufferSize);
                        send = send.substr(bufferSize,send.length() - bufferSize);
                    }
                    else{
                        temp = send.substr(0,send.length());
                        send = "";
                    }
                    strcpy(buf,temp.c_str());
                    write(fd2_worker,buf,bufferSize);
                }
                strcpy(buf,dolla.c_str());
                write(fd2_worker,buf,1);
            }
        }
        else if (answer1 == "/topk-AgeRanges"){
            string k;
            string country;
            string virusName;
            string date1;
            string date2;
            ss >> k;
            ss >> country;
            ss >> virusName;
            ss >> date1;
            ss >> date2;
            if (virusName == "" || date1 == "" || date2 == "" || k == "" || country == ""){
                fail++;
                strcpy(buf,dolla.c_str());
                write(fd2_worker,buf,1);
            }
            else{
                success++;
                string send = "";
                for (int i = 0;i < count_of_countries; i ++){
                    if (country == countries[i]){
                        send = buckets[i]->topk_AgeRanges(k,virusName,date1,date2);
                        break;
                    }
                }
                
                int message_length = send.length();
                int times = message_length/bufferSize;
                if (message_length % bufferSize != 0 || times == 0)
                        times++;
                string temp;
                for (int j = 0;j<times;j++){
                    if (bufferSize < send.length()){
                        temp = send.substr(0,bufferSize);
                        send = send.substr(bufferSize,send.length() - bufferSize);
                    }
                    else{
                        temp = send.substr(0,send.length());
                        send = "";
                    }
                    strcpy(buf,temp.c_str());
                    write(fd2_worker,buf,bufferSize);
                }
                strcpy(buf,dolla.c_str());
                write(fd2_worker,buf,1);
            }
        }
        else if (answer1 == "/searchPatientRecord"){
            string recordID;
            ss >> recordID;
            if (recordID == ""){
                fail++;
                strcpy(buf,dolla.c_str());
                write(fd2_worker,buf,1);
            }
            else{
                success++;
                string send ="";
                for (int i = 0; i < count_of_countries; i++){
                    send = records_list[i]->searchPatientRecord(recordID);
                    if (send != "")
                        break;
                }
                int message_length = send.length();
                int times = message_length/bufferSize;
                if (message_length % bufferSize != 0 || times == 0)
                        times++;
                string temp;
                for (int j = 0;j<times;j++){
                    if (bufferSize < send.length()){
                        temp = send.substr(0,bufferSize);
                        send = send.substr(bufferSize,send.length() - bufferSize);
                    }
                    else{
                        temp = send.substr(0,send.length());
                        send = "";
                    }
                    strcpy(buf,temp.c_str());
                    write(fd2_worker,buf,bufferSize);
                }
                strcpy(buf,dolla.c_str());
                write(fd2_worker,buf,1);
            }
        }
        else if (answer1 == "/numPatientAdmissions"){
           
            string virusName;
            string date1;
            string date2;
            string country;
            ss >> virusName;
            ss >> date1;
            ss >> date2;
            ss >> country;

            if (virusName == "" || date1 == "" || date2 == ""){
                fail++;
                strcpy(buf,dolla.c_str());
                write(fd2_worker,buf,1);
            }
            else{
                success++;
                int sum=0;
                string send = "";
                if (country == ""){
                    for (int i = 0;i < count_of_countries; i++){
                        sum = buckets[i]->diseaseFrequency(date1,date2,virusName);
                        send = send + countries[i] + " " + to_string(sum) + "\n";
                    }
                }
                else{
                    for (int i = 0; i < count_of_countries; i++){
                        if (country == countries[i]){
                            sum = buckets[i]->diseaseFrequency(date1,date2,virusName);
                            send = countries[i] + " " + to_string(sum) + "\n";
                            break;
                        }
                    }
                }
                
                int message_length = send.length();
                int times = message_length/bufferSize;
                if (message_length % bufferSize != 0 || times == 0)
                        times++;
                string temp;
                for (int j = 0;j<times;j++){
                    if (bufferSize < send.length()){
                        temp = send.substr(0,bufferSize);
                        send = send.substr(bufferSize,send.length() - bufferSize);
                    }
                    else{
                        temp = send.substr(0,send.length());
                        send = "";
                    }
                    strcpy(buf,temp.c_str());
                    write(fd2_worker,buf,bufferSize);
                }
                strcpy(buf,dolla.c_str());
                write(fd2_worker,buf,1);
            }
        }
        else if (answer1 == "/numPatientDischarges"){
            string virusName;
            string date1;
            string date2;
            string country;
            ss >> virusName;
            ss >> date1;
            ss >> date2;
            ss >> country;

            if (virusName == "" || date1 == "" || date2 == ""){
                fail++;
                strcpy(buf,dolla.c_str());
                write(fd2_worker,buf,1);
            }
            else{
                success++;
                int sum=0;
                string send = "";
                if (country == ""){
                    for (int i = 0;i < count_of_countries; i++){
                        sum = buckets[i]->numPatientDischarges(date1,date2,virusName);
                        send = send + countries[i] + " " + to_string(sum) + "\n";
                    }
                }
                else{
                    for (int i = 0; i < count_of_countries; i++){
                        if (country == countries[i]){
                            sum = buckets[i]->numPatientDischarges(date1,date2,virusName);
                            send = countries[i] + " " + to_string(sum) + "\n";
                            break;
                        }
                    }
                }
                
                int message_length = send.length();
                int times = message_length/bufferSize;
                if (message_length % bufferSize != 0 || times == 0)
                        times++;
                string temp;
                for (int j = 0;j<times;j++){
                    if (bufferSize < send.length()){
                        temp = send.substr(0,bufferSize);
                        send = send.substr(bufferSize,send.length() - bufferSize);
                    }
                    else{
                        temp = send.substr(0,send.length());
                        send = "";
                    }
                    strcpy(buf,temp.c_str());
                    write(fd2_worker,buf,bufferSize);
                }
                strcpy(buf,dolla.c_str());
                write(fd2_worker,buf,1);
            }

        }
        if (sig_usr1 == 1){
            sigurs1_fun();
            sig_usr1 = 0;
        }

        


    }

    return 0; 
	
}



long int totalDays(string date){
    if (date == "-")
        return -1;
    string tempString;
    tempString = date.substr(0,2);
    int days = atoi(tempString.c_str());
    tempString = date.substr(3,2);
    int months = atoi(tempString.c_str());
    tempString = date.substr(6,4);
    int years = atoi(tempString.c_str());

    long int x = years*365 + months * 30 + days;        //convert to days
    return x;
}

