CXXFLAGS := -std=c++11
diseaseAggregator:./src/ex2.o ./src/worker.o ./src/list.o ./src/bucketList.o
	g++ -o diseaseAggregator -std=c++11 ./src/ex2.o ./src/worker.o ./src/list.o ./src/bucketList.o -g3

ex2.o:./src/ex2.cpp ./Headers/ex2.h
	g++ -std=c++11 -c./src/ex2.o ./src/ex2.cpp -g3

worker.o:./src/worker.cpp ./Headers/ex2.h
	g++ -std=c++11 -c ./src/worker.o ./src/worker.cpp -g3

list.o:./src/list.cpp ./Headers/ex2.h
	g++ -std=c++11 -c ./src/list.o ./src/list.cpp -g3

bucketList.o:./src/bucketList.cpp ./Headers/ex2.h
	g++ -std=c++11 -c ./src/bucketList.o ./src/bucketList.cpp -g3


clean:
	rm -f ./diseaseAggregator 
	rm -f ./worker 
	rm -f ./src/*.o
