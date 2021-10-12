#include "../Headers/ex2.h"



int global_numWorkers = 0;
string* countries_parent;
int* pids;
int glob_buff_size;
char* in_dir;
int* country_pids;
int number_of_countries;
int *fd1;
int *fd2;
pid_t global_pid;


volatile sig_atomic_t sig_child_death = 0;
volatile sig_atomic_t sig_flag = 0;

void signalHandler( int signum ) {

	if (signum == SIGINT){
		
		close(0);													//close stdin because it will stuck on get line and we dont want that 
		signal(SIGINT,SIG_IGN);
		sig_flag = 1;
	}
	if (signum == SIGQUIT){
		
		close(0);													//close stdin because it will stuck on get line and we dont want that 
		signal(SIGQUIT,SIG_IGN);
		sig_flag = 1;
	}

}

void child_death(int signum){

	pid_t p;
    int status;

    while ((p=waitpid(-1, &status, WNOHANG)) != -1)
    { 
    	if (p == 0)
    		return;
    	else if (p == -1)
    		return;
    	else{

    		sig_child_death = 1;
    		global_pid = p;
    	}
    }
}


void new_worker(pid_t p){
	int target_index;
	for (int i = 0;i < global_numWorkers; i++){
		if (p == pids[i]){
			target_index = i;
			break;
		}
	}
	string str1="/tmp/PtW" + to_string(target_index);
	string str2="/tmp/WtP" + to_string(target_index);
	close(fd1[target_index]);
	close(fd2[target_index]);
	unlink(str1.c_str());
	unlink(str2.c_str());
	if (mkfifo(str1.c_str(), 0666) < 0)
        perror("mkfifo1");
    if (mkfifo(str2.c_str(),0666) < 0)
   		perror("mkfifo2");
   	int temp_index = target_index;
	pids[target_index] = fork();
	char buf[glob_buff_size];

	if (pids[target_index] == 0){									//child
		worker(temp_index,str1,str2,glob_buff_size,in_dir);
		exit(0);
	} 					
	else{															//parent process
		if ((fd1[target_index] = open(str1.c_str(), O_WRONLY)) < 0)
	        	perror("parent - open");
	    if ((fd2[target_index] = open(str2.c_str(), O_RDONLY)) < 0)
	        	perror("parent - open");
		string countries_string="";
		
	    for (int j = 0;j < number_of_countries;j++){
	   		if (country_pids[j] == target_index){			      		
	   			countries_string = countries_string +" "+ countries_parent[j];	
	        }
	    }
	    int message_length = countries_string.length();
	    int times = message_length/glob_buff_size;
	    if (message_length % glob_buff_size != 0 || times == 0)
	       	times++;
	    string temp;
	    for (int j = 0;j<times;j++){
	   		if (glob_buff_size < countries_string.length()){
	        	temp = countries_string.substr(0,glob_buff_size);
	        	countries_string = countries_string.substr(glob_buff_size,countries_string.length() - glob_buff_size);
	        }
	        else{
	        	temp = countries_string.substr(0,countries_string.length());
	        	countries_string = "";
	        }
	        
	        strcpy(buf,temp.c_str());
	        write(fd1[temp_index],buf,glob_buff_size);
	    }
	    string dolla = "$";
	    strcpy(buf,dolla.c_str());
	    write(fd1[temp_index],buf,glob_buff_size);
	    

	    char out[glob_buff_size];
	    string message="";
	    int num;
	    while ((num = read(fd2[temp_index],out,glob_buff_size)) > 0){
        	out[num] = '\0';
        	message = out;
        	if (message == "$"){
        		cout << "I am the new worker with index " << target_index << " and i finished my work" << endl;
        		break;
        	}
    			cout << message;
    	}

	}

}


int main(int argc, char *argv[]){
	char fff[2]="$";
	int numWorkers = atoi(argv[2]);
	int bufferSize = atoi(argv[4]);
	char input_dir[30];
	
	strcpy(input_dir,argv[6]);
	in_dir = argv[6];					//global input_dir
	global_numWorkers = numWorkers;
	glob_buff_size = bufferSize;
	cout << "Workers are " << numWorkers << " buffer size is " << bufferSize << " and input directory is " << input_dir << "\n";
	DIR *dir;
	dir = opendir(input_dir);
	struct dirent *entry;
	if (!dir){
		cout << "Directory not found\n";
		return 0;
	}

	number_of_countries = 0;
	
	while ((entry = readdir(dir)) != NULL){
		if (entry->d_name[0] != '.'){
			
			number_of_countries++;
		}
	}

	
	countries_parent = new string[number_of_countries];
	int x =0;
	closedir(dir);
	dir = opendir(input_dir);
	while ((entry = readdir(dir)) != NULL){
		if (entry->d_name[0] != '.'){
			countries_parent[x++] = entry->d_name;
			
		}
	}
	closedir(dir);
	if (number_of_countries < numWorkers)					//we dont need more workers than subdirectories because they will do nothing
		numWorkers = number_of_countries;

	country_pids = new int[number_of_countries];
	int temp_worker = 0;

	for (int i = 0; i < number_of_countries;i++){
		country_pids[i] = temp_worker++;
		if (temp_worker == numWorkers)
			temp_worker = 0;
	}





    int num, fd;
    char* buf = new char[bufferSize + 1];									//need one more byte for terminal char '\0'
    memset(buf,0,bufferSize);																			//initialize buffer because valgrind complains
    fd1 = new int[numWorkers];
    fd2 = new int[numWorkers];
    pid_t pid;
    int temp_index;
    string dolla ="$";
    string str1[numWorkers];
    string str2[numWorkers];
    pids = new int[numWorkers];
    for (int i=0;i<numWorkers;i++){
    	str1[i] = "/tmp/PtW" + to_string(i);									//PtW stands for Parent to worker (parent will write to worker on this pipe)
    	str2[i] = "/tmp/WtP" + to_string(i);									//WtP stands for Worker to parent (worker will write to parent on this pipe)
   		if (mkfifo(str1[i].c_str(), 0666) < 0)
        	perror("mkfifo1");
        if (mkfifo(str2[i].c_str(),0666) < 0)
        	perror("mkfifo2");
		temp_index = i;
		pids[i] = fork();
		pid = pids[i];
		if (pid == 0)
			break;
		
	}

    
    if (pid == 0)
    {
    	delete[] fd1;											//delete duplicated allocations because child does not need them
    	delete[] fd2;
    	delete[] country_pids;
	 	delete[] countries_parent;
	 	delete[] pids;
	 	delete[] buf;

       	worker(temp_index,str1[temp_index],str2[temp_index],bufferSize,input_dir);
       	
        exit(0);

        
        
    }
    else
    {	signal(SIGCHLD,child_death);
    	signal(SIGINT,signalHandler);
		signal(SIGQUIT,signalHandler);
		
		
        for (int i=0;i<numWorkers;i++){
        	string countries_string="";
        	
	       
        
	        if ((fd1[i] = open(str1[i].c_str(), O_WRONLY)) < 0)
	        	perror("parent - open");
	        for (int j = 0;j < number_of_countries;j++){
	        	if (country_pids[j] == i){			      		
	        		countries_string = countries_string +" "+ countries_parent[j];	
	        	}

	        }
	        int message_length = countries_string.length();
	        int times = message_length/bufferSize;
	        if (message_length % bufferSize != 0 || times == 0)
	        		times++;
	        string temp;
	        for (int j = 0;j<times;j++){
	        	if (bufferSize < countries_string.length()){
	        		temp = countries_string.substr(0,bufferSize);
	        		countries_string = countries_string.substr(bufferSize,countries_string.length() - bufferSize);
	        	}
	        	else{
	        		temp = countries_string.substr(0,countries_string.length());
	        		countries_string = "";
	        	}
	        	strcpy(buf,temp.c_str());
	        	

	        	write(fd1[i],buf,bufferSize);
	        }
	        strcpy(buf,"$");
	        write(fd1[i],buf,1);
	       

	      
    	}
    	
    	char out[bufferSize];
        
    	for (int i=0;i<numWorkers;i++){
    	

    		if ((fd2[i] = open(str2[i].c_str(), O_RDONLY)) < 0)
	        	perror("parent - open");
	        char out[bufferSize];
	        string message="";
	        
	        while ((num = read(fd2[i],out,bufferSize)) > 0){
        		out[num] = '\0';
        		string temp123 = out;
        		
        		if (temp123 == "$"){
        			cout << "I am worker with index " << i << " and i finished my work" << endl;
        			break;
        		}
        		message = message + out;
    		}
        	cout << message;
    		
    		
    		
    	}
    	
    	//QUERIES
    	string answer;
    	string answer1;
    	cout << "Give option :) " << endl;
    	
    	int success = 0;
    	int fail = 0;
    	while ( getline(cin, answer)){
    		
    		if (sig_child_death == 1){
				
				new_worker(global_pid);
				sig_child_death = 0;
			}

    		answer1="";
    		istringstream ss(answer);
			ss >> answer1;
			if (answer1 == "/exit"){
				success++;
				cout << "exiting" << endl;
				signal(SIGCHLD,SIG_IGN);
				sig_flag = 1;
				
			}
			else if (answer1 == "/listCountries"){
				success++;
				for (int i = 0; i < number_of_countries;i++){
					for (int j = 0;j< numWorkers; j++){
						if (country_pids[i] == j)
							cout << countries_parent[i] << " " << pids[j] << endl;
					}
				}
				
				
			}
			else if (answer1 == "/diseaseFrequency"){
				
				
				string virusName;
				string date1;
				string date2;
				string country;
				ss >> virusName;
				ss >> date1;
				ss >> date2;
				ss >> country;
				
				if (country == ""){
					success++;
					for (int i = 0; i < numWorkers; i++){
						
	        			string message = answer;
	        			int message_length = message.length();
	        			int times = message_length/bufferSize;
	        			if (message_length % bufferSize != 0 || times == 0)
			        		times++;
			        	string temp;
			        	for (int j = 0;j<times;j++){
				        	if (bufferSize < message.length()){
				        		temp = message.substr(0,bufferSize);
				        		message = message.substr(bufferSize,message.length() - bufferSize);
				        	}
				        	else{
				        		temp = message.substr(0,message.length());
				        		message = "";
				        	}
				        	
				        	strcpy(buf,temp.c_str());
				        	write(fd1[i],buf,bufferSize);
				        }
				        strcpy(buf,"$");

				        write(fd1[i],buf,1);
				        

					}
					
					int sum = 0;
					for (int i = 0; i < numWorkers; i++){
						
						char out[bufferSize];
				        string message="";
				       
				        while ((num = read(fd2[i],out,bufferSize)) > 0){
			        		out[num] = '\0';
			        		string temp123 = out;
			        		if (temp123 == "$")
			        			break;
			        		message = message + out;
			        		
			    		}
			    		
			    		sum = sum + atoi(message.c_str());

			    		
					}
			        cout << sum << endl;
			        
				}
				else{
					
					int worker_num = -1;
					for (int i = 0; i < number_of_countries && worker_num == -1; i++){
						for (int j = 0; j < numWorkers && worker_num == -1; j++){
							if (country == countries_parent[i] && country_pids[i] == j){
								
								worker_num = j;
								string message = answer;
			        			int message_length = message.length();
			        			int times = message_length/bufferSize;
			        			if (message_length % bufferSize != 0 || times == 0)
					        		times++;
					        	string temp;
					        	for (int c = 0;c<times;c++){
						        	if (bufferSize < message.length()){
						        		temp = message.substr(0,bufferSize);
						        		message = message.substr(bufferSize,message.length() - bufferSize);
						        	}
						        	else{
						        		temp = message.substr(0,message.length());
						        		message = "";
						        	}
						        	strcpy(buf,temp.c_str());
						        	write(fd1[worker_num],buf,bufferSize);
						        }
						        strcpy(buf,dolla.c_str());
						        write(fd1[worker_num],buf,bufferSize);
							}
						}
					}
					if (worker_num != -1){
						success++;
						char out[bufferSize];
						char out2[bufferSize];
				        string message="";
				        int sum = 0;
				        int num2 = 0;
				        string message2="";
				        
				        while ((num = read(fd2[worker_num],out,bufferSize)) > 0){
			        		out[num] = '\0';
			        		string temp123 = out;															//then sigurs 1 is activated and we should read statistics
			        		if (temp123 == "$")
			        			break;
			        		message = message + out;
			    		}
			    		
			    		sum = sum + atoi(message.c_str());
			    		
			    		cout << sum << endl;
			    	}
			    	else{
			    		fail++;
			    	}

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

				int worker_num = -1;
				for (int i = 0; i < number_of_countries && worker_num == -1; i++){
					for (int j = 0; j < numWorkers && worker_num == -1; j++){
						if (country == countries_parent[i] && country_pids[i] == j){
							
							worker_num = j;
							string message = answer;
		        			int message_length = message.length();
		        			int times = message_length/bufferSize;
		        			if (message_length % bufferSize != 0 || times == 0)
				        		times++;
				        	string temp;
				        	for (int c = 0;c<times;c++){
					        	if (bufferSize < message.length()){
					        		temp = message.substr(0,bufferSize);
					        		message = message.substr(bufferSize,message.length() - bufferSize);
					        	}
					        	else{
					        		temp = message.substr(0,message.length());
					        		message = "";
					        	}
					        	strcpy(buf,temp.c_str());
					        	write(fd1[worker_num],buf,bufferSize);
					        }
					        strcpy(buf,dolla.c_str());
					        write(fd1[worker_num],buf,1);
						}
					}
				}

				char out[bufferSize];
		        string message="";
		        int sum = 0;
		      
		        if (worker_num != -1){
		        	success++;
			        while ((num = read(fd2[worker_num],out,bufferSize)) > 0){
		        		out[num] = '\0';
		        		string temp123 = out;
		        		if (temp123 == "$")
		        			break;
		        		message = message + out;
		        		
		    		}
		    		
		    		cout << message << endl;
	    		}
	    		else{
	    			fail++;
	    		}
			}
			else if (answer1 == "/searchPatientRecord"){
				success++;

				for (int i = 0; i < numWorkers; i++){
        			string message = answer;
        			int message_length = message.length();
        			int times = message_length/bufferSize;
        			if (message_length % bufferSize != 0 || times == 0)
		        		times++;
		        	string temp;
		        	for (int j = 0;j<times;j++){
			        	if (bufferSize < message.length()){
			        		temp = message.substr(0,bufferSize);
			        		message = message.substr(bufferSize,message.length() - bufferSize);
			        	}
			        	else{
			        		temp = message.substr(0,message.length());
			        		message = "";
			        	}
			        	
			        	strcpy(buf,temp.c_str());
			        	write(fd1[i],buf,bufferSize);
			        }
			        strcpy(buf,dolla.c_str());
			        write(fd1[i],buf,1);
			      
				}

				for (int i = 0; i < numWorkers; i++){
					char out[bufferSize];
			        string message="";
			        int sum = 0;
			       
			        while ((num = read(fd2[i],out,bufferSize)) > 0){
		        		out[num] = '\0';
		        		string temp123 = out;
		        		if (temp123 == "$")
		        			break;
		        		message = message + out;
		        		
		    		}
		    		if (message != "")
		    			cout << message << endl;
				}
			}
			else if(answer1 == "/numPatientAdmissions"){
				
				
				string virusName;
				string date1;
				string date2;
				string country;
				ss >> virusName;
				ss >> date1;
				ss >> date2;
				ss >> country;
				if (country == ""){
					success++;
					for (int i = 0; i < numWorkers; i++){
						
	        			string message = answer;
	        			int message_length = message.length();
	        			int times = message_length/bufferSize;
	        			if (message_length % bufferSize != 0 || times == 0)
			        		times++;
			        	string temp;
			        	for (int j = 0;j<times;j++){
				        	if (bufferSize < message.length()){
				        		temp = message.substr(0,bufferSize);
				        		message = message.substr(bufferSize,message.length() - bufferSize);
				        	}
				        	else{
				        		temp = message.substr(0,message.length());
				        		message = "";
				        	}
				        	strcpy(buf,temp.c_str());
				        	write(fd1[i],buf,bufferSize);
				        }
				        strcpy(buf,dolla.c_str());
				        write(fd1[i],buf,1);
				        //close(fd1);

					}
					
					int sum = 0;
					for (int i = 0; i < numWorkers; i++){
						
						char out[bufferSize];
				        string message="";
				        
				        while ((num = read(fd2[i],out,bufferSize)) > 0){
			        		out[num] = '\0';
			        		string temp123 = out;
			        		if (temp123 == "$")
			        			break;
			        		message = message + out;
			        		
			    		}
			    		
			    		cout << message;

					}
			        
			    }
			    else{
					
					int worker_num = -1;
					for (int i = 0; i < number_of_countries && worker_num == -1; i++){
						for (int j = 0; j < numWorkers && worker_num == -1; j++){
							if (country == countries_parent[i] && country_pids[i] == j){
								
								worker_num = j;
								string message = answer;
			        			int message_length = message.length();
			        			int times = message_length/bufferSize;
			        			if (message_length % bufferSize != 0 || times == 0)
					        		times++;
					        	string temp;
					        	for (int c = 0;c<times;c++){
						        	if (bufferSize < message.length()){
						        		temp = message.substr(0,bufferSize);
						        		message = message.substr(bufferSize,message.length() - bufferSize);
						        	}
						        	else{
						        		temp = message.substr(0,message.length());
						        		message = "";
						        	}
						        	strcpy(buf,temp.c_str());
						        	write(fd1[worker_num],buf,bufferSize);
						        }
						        strcpy(buf,dolla.c_str());
						        write(fd1[worker_num],buf,1);
							}
						}
					}
					if (worker_num != -1){
						success++;
						char out[bufferSize];
				        string message="";
				        int sum = 0;
				        
				        while ((num = read(fd2[worker_num],out,bufferSize)) > 0){
			        		out[num] = '\0';
			        		string temp123 = out;
			        		if (temp123 == "$")
			        			break;
			        		message = message + out;
			        		
			    		}
			    		cout << message;
			    	}
			    	else{
			    		fail++;
			    	}
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
				if (country == ""){
					success++;
					for (int i = 0; i < numWorkers; i++){
						
	        			string message = answer;
	        			int message_length = message.length();
	        			int times = message_length/bufferSize;
	        			if (message_length % bufferSize != 0 || times == 0)
			        		times++;
			        	string temp;
			        	for (int j = 0;j<times;j++){
				        	if (bufferSize < message.length()){
				        		temp = message.substr(0,bufferSize);
				        		message = message.substr(bufferSize,message.length() - bufferSize);
				        	}
				        	else{
				        		temp = message.substr(0,message.length());
				        		message = "";
				        	}
				        	strcpy(buf,temp.c_str());
				        	write(fd1[i],buf,bufferSize);
				        }
				        strcpy(buf,dolla.c_str());
				        write(fd1[i],buf,bufferSize);
				        //close(fd1);

					}
					
					int sum = 0;
					for (int i = 0; i < numWorkers; i++){
						
						char out[bufferSize];
				        string message="";
				        
				        while ((num = read(fd2[i],out,bufferSize)) > 0){
			        		out[num] = '\0';
			        		string temp123 = out;
			        		if (temp123 == "$")
			        			break;
			        		message = message + out;
			        		
			    		}
			    		
			    		cout << message;

					}
			        
			    }
			    else{
					int worker_num = -1;
					for (int i = 0; i < number_of_countries && worker_num == -1; i++){
						for (int j = 0; j < numWorkers && worker_num == -1; j++){
							if (country == countries_parent[i] && country_pids[i] == j){
								
								worker_num = j;
								string message = answer;
			        			int message_length = message.length();
			        			int times = message_length/bufferSize;
			        			if (message_length % bufferSize != 0 || times == 0)
					        		times++;
					        	string temp;
					        	for (int c = 0;c<times;c++){
						        	if (bufferSize < message.length()){
						        		temp = message.substr(0,bufferSize);
						        		message = message.substr(bufferSize,message.length() - bufferSize);
						        	}
						        	else{
						        		temp = message.substr(0,message.length());
						        		message = "";
						        	}
						        	strcpy(buf,temp.c_str());
						        	write(fd1[worker_num],buf,bufferSize);
						        }
						        strcpy(buf,dolla.c_str());
						        write(fd1[worker_num],buf,bufferSize);
							}
						}
					}
					if (worker_num != -1){
						char out[bufferSize];
				        string message="";
				        int sum = 0;
				        
				        while ((num = read(fd2[worker_num],out,bufferSize)) > 0){
			        		out[num] = '\0';
			        		string temp123 = out;
			        		if (temp123 == "$")
			        			break;
			        		message = message + out;
			    		}
			    		cout << message;
			    	}
			    	else{
			    		fail++;
			    	}
				}
			}
			else
				fail++;

			if (sig_flag == 1){
				for (int i = 0; i < numWorkers; i++){
					kill(pids[i],SIGKILL);
					
				}
				ofstream file;
		        string f_name = "log_file." + to_string(getpid());
		        file.open(f_name);
		        for (int i = 0; i < number_of_countries; i++){
		            file << countries_parent[i];
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
		        break;
			}

			
		}

 	}
 	delete[] country_pids;
 	delete[] countries_parent;
 	delete[] pids;
 	delete[] buf;
 	
    for (int i=0;i<numWorkers;i++){
    	close(fd1[i]);
    	close(fd2[i]);
    	
    	cout << "Unlinking" << str1[i] << endl;
    	unlink(str1[i].c_str());
    	cout << "Unlinking" << str2[i] << endl;
    	unlink(str2[i].c_str());
	}
	delete[] fd1;
    delete[] fd2;
    return 0;
}