#include <stdio.h>
#include <iostream>
#include <memory>
#include <stdlib.h>
#include <string.h>
#include <vector>
#include <mutex>
#include <unordered_map>
#include <math.h>
#include "ThreadPool.h"

using namespace std;

#define TREE_TYPE 2
#define BW_CORE 6  //6GB/s/core == 6B/ns
#define LAT_CORE 200
#define NETWORK_LAT 3810 // send 0 Byte ns
#define NUM_OF_THREADS 8 // The number of threads to find critical path
                         // One thread per core
unordered_map<int, unordered_map<int, bool>> graph;
unordered_map<int, unordered_map<int, int>> mylevel;
unordered_map<int, unordered_map<int, std::unique_ptr<std::mutex>>> mylevelMutexs;
std::unique_ptr<std::mutex> path_mutex;

int maxcol;
int maxpathlength;
int NPROW;
int NPCOL;
struct Cell{
    int col;
    int row;
    Cell(int i, int j) {
        col = i;
        row = j;
    }
    Cell() {
        col = 0;
        row = 0;
    }
};
vector<vector<Cell>> path;

bool exist_in_map(unordered_map<int, unordered_map<int, bool>>& hash_map, int row, int col) {
    auto row_it = hash_map.find(row);
    if (row_it == hash_map.end())
        return false;
    auto col_it = row_it->second.find(col);
    if (col_it == row_it->second.end())
        return false;

    return true;
}

void find_level(Cell point, int l) {
        l += 1;

        if (point.col >= maxcol && point.col == point.row) {
         	if (mylevel[point.col][point.col] < l){
         		mylevel[point.col][point.col] = l;
	 	    }
                return;
        }


        int cur_col = point.col;
        int row_end = point.row;

        if (point.row == point.col) {
		row_end += 1;
		while (row_end <= maxcol){
		    if (graph[row_end][cur_col] == 1) {
		        if (mylevel[row_end][cur_col] < l){
             			mylevel[row_end][cur_col] = l;
		        }
		        Cell new_point(row_end, cur_col);
		        find_level(new_point, l);
		    }
		    row_end++;
       		}
	    }


        if (point.row > point.col) {
		    if (mylevel[point.row][cur_col] < l){
    			mylevel[point.row][cur_col] = l;
		    }
		    Cell new_point(point.row, point.row);
		    find_level(new_point, l);
        }
       return;
}

void find_path(Cell point, vector<Cell> local_path, int l) {
#ifdef DEBUG_1
    cout << "Enter find_path, point.col = " << point.col << ", point.row = " << point.row << endl;
    cout.flush();
#endif
    local_path.push_back(point);
    l += 1;


    if (point.col >= maxcol && point.col == point.row) {
        // lock path and maxpathlength since they are shared among threads
        path_mutex->lock();
        if (maxpathlength < local_path.size()) {
            path.push_back(local_path);
            maxpathlength = local_path.size();
#ifdef DEBUG_1
            cout << "find a path, size = " << local_path.size()
                 << " current max = " << maxpathlength
                 << " current total path num = " <<  path.size() <<endl;
            cout.flush();
#endif
        } else {
            local_path.clear();
        }
        // unlock path and maxpathlength
        path_mutex->unlock();
        return;
    }


    int cur_col = point.col;
    int row_end = point.row;
    vector<Cell> new_cells;

    if (point.row == point.col) {
        row_end += 1;
#ifdef DEBUG_1
        cout << "At diag, this point row_end = " << row_end << endl;
        cout.flush();
#endif
		while (row_end <= maxcol) {
            if (exist_in_map(graph, row_end, cur_col)) {
                // lock the mylevel using its mutex
                //cout << "lock the level row_end cur_col" << endl;
                mylevelMutexs[row_end][cur_col]->lock();
                int cur_level = mylevel[row_end][cur_col];
                if (cur_level < l) {
                    mylevel[row_end][cur_col] = l;
                    new_cells.emplace_back(cur_col, row_end);
#ifdef DEBUG_1
            	    cout << "At diag, find new child, this point (" << point.row << ") find children (row col) = (" << row_end << "," << cur_col << ")" << endl;
                    cout.flush();
#endif
                } else {
#ifdef DEBUG_1
                    cout << "omit  a child" << endl;
#endif
                }
                // unlock the mylevel[row_end][cur_col]
                //cout << "unlock mylevel row_end cur_col" << endl;
                mylevelMutexs[row_end][cur_col]->unlock();
            }
            row_end++;
		}


#ifdef DEBUG_1
        cout << "At diag, this point (" << point.row << ") find children total = " << new_cells.size() << endl;
	    cout.flush();
#endif
        if (new_cells.size() == 0) {
            // lock path and maxpathlength
            path_mutex->lock();
            if (maxpathlength < local_path.size()) {
                path.push_back(local_path);
                maxpathlength = local_path.size();
#ifdef DEBUG_1
                cout << "find a path, size = " << local_path.size() << " current max = "<< maxpathlength << " current total path num = " <<  path.size() <<endl;
                cout.flush();
#endif
            } else {
                local_path.clear();
            }
            // unlock path and maxpathlength
            path_mutex->unlock();
        }


        for (int i = 0; i < new_cells.size(); i++) {
#ifdef DEBUG_1
    	    cout << "I am here 2.4, start " << new_cells[i].row << " , " << new_cells[i].col << endl;
            cout.flush();
#endif

            find_path(new_cells[i], local_path, l);
#ifdef DEBUG_1
            cout << "I am here 2.4, end " << new_cells[i].row << " , " << new_cells[i].col << endl;
            cout.flush();
#endif
        }
    } // if point.row=point.col on diag

    if (point.col < point.row) {
#ifdef DEBUG_1
       cout << "At off-diag, this point = (" << point.col << " , " << point.row << " ), level = " << mylevel[point.row][point.row] <<endl;
       cout.flush();
#endif
       Cell new_point(point.row, point.row);
       // lock mylevel
       mylevelMutexs[point.row][point.col]->lock();
       if (mylevel[point.row][point.row] < l) {
           mylevel[point.row][point.row] = l;
       }
       mylevelMutexs[point.row][point.col]->unlock();
#ifdef DEBUG_1
       cout << "At off-diag, find new child, start " << point.row << " , " << point.row << endl;
       cout.flush();
#endif
       find_path(new_point, local_path, l);
#ifdef DEBUG_1
       cout << "I am here 2.6, end " << point.row << " , " << point.row << endl;
       cout.flush();
#endif
    }
#ifdef DEBUG_1
    cout << "I am here 2.6 point.col = " << point.col << ", point.row = " << point.row << endl;
    cout.flush();
#endif
    return;
}

float upper_power_of_two(int v){
    float network_bw[15]={0.53,1.06,2.12,4.24,8.49,16.98,34.15,68.23,134.80,265.59,388.50,617.77,935.02,1765.46,3518.02};  //,6821.06,7441.23,7754.30,7931.55,8019.80,8074.51,8097.14,8079.97};
    //float network_bw[15]={2.34382,4.41335,8.54904,20.3288,39.0057,72.4541,125.623,272.984,448.888,666.835,581.516,830.467,1612.09,2765.46, 4518.02}; //haswell
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    v++;

    //cout << v <<"," << log(v) << network_bw[(int)log2(v)]  << endl<< std::flush;
    return network_bw[(int)log2(v)-1]/1e3;
    //return v;
}


float fompiput_upper_power_of_two(int v)
{
    float fompi[15]={3.27888,6.7164,12.3539,23.4398,46.1358,85.1349,152.238,274.504,421.291,831.737,1112.8,2048.55,2528.43,3209.58,6120.28};
    //float fompi[15]={5.19148,10.0762,21.0765,39.6508,83.754,149.342,279.648,414.555,881.995,1281.71,1736.03,2231.95,2430.68,2869.23,6234.01}; //haswell
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    v++;

    //cout << v <<"," << log(v) << network_bw[(int)log2(v)]  << endl<< std::flush;
    return fompi[(int)log2(v)-1]/1e3;
    //return v;
}


float fompiget_upper_power_of_two(int v)
{
    float fompi[15]={3.27888,6.7164,12.3539,23.4398,46.1358,85.1349,152.238,274.504,421.291,831.737,1112.8,2048.55,2528.43,3209.58,6120.28};
    //float fompi[15]={5.19148,10.0762,21.0765,39.6508,83.754,149.342,279.648,414.555,881.995,1281.71,1736.03,2231.95,2430.68,2869.23,6234.01}; //haswell
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    v++;

    //cout << v <<"," << log(v) << network_bw[(int)log2(v)]  << endl<< std::flush;
    return fompi[(int)log2(v)-1]/1e3;
    //return v;
}


float fompicounter_upper_power_of_two(int v)
{
    float fompi_counter[15]={0.415952,0.728321,1.60955,3.11724,5.76481,12.0821,22.6828,49.2828,89.8562,165.587,335.04,674.643,1497.37,1993.54,2061.98};
    //float fompi_counter[15]={0.895065,1.58806,2.79401,6.21149,13.6465,26.819,47.7808,101.066,193.221,379.592,444.028,701.86,1421.58,2382.34,3986.8}; //haswell
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    v++;

    //cout << v <<"," << log(v) << network_bw[(int)log2(v)]  << endl<< std::flush;
    return fompi_counter[(int)log2(v)-1]/1e3;
    //return v;
}

/* model_message_time (int BC=0/RD=1), int twoside=0/fompiput=2/fompiget=2/nvshmemget=3, int mywidth, int myheight, int messagecnt)*/
double model_message_time(int commu_type, int implement_type, int mywidth, int myheight, int msgcnt){
    double time=0.0;
    double myBW=0.0;

    if (commu_type == 0 ) {
        if (implement_type == 0) {
            //two sided
            myBW = upper_power_of_two(mywidth * 8);
#ifdef DEBUG_2
            cout << "commu_type=" << commu_type << ", implement_type=" << implement_type  << ", myBW="<<  myBW <<endl;
            cout.flush();
#endif
        }else if (implement_type == 1) {
            // fompi_put
            myBW = fompiput_upper_power_of_two(mywidth * 8);
#ifdef DEBUG_2
            cout << "commu_type=" << commu_type << ", implement_type=" << implement_type  << ", myBW="<<  myBW <<endl;
            cout.flush();
#endif
        }else if (implement_type == 2) { // fompi_get
            myBW = fompiget_upper_power_of_two(mywidth * 8);
#ifdef DEBUG_2
            cout << "commu_type=" << commu_type << ", implement_type=" << implement_type  << ", myBW="<<  myBW <<endl;
            cout.flush();
#endif
        }

        if (NPROW >= 8) {
            time += NETWORK_LAT + ceil(log2(msgcnt) * mywidth * 8 / myBW);
#ifdef DEBUG_2
            cout << "(NPROW>=8) time=" << time << endl;
            cout.flush();
#endif
        } else {
            time += NETWORK_LAT + ceil(msgcnt * mywidth * 8 / myBW);
#ifdef DEBUG_2
            cout << "(NPROW<8) time=" << time << endl;
            cout.flush();
#endif
        }
    } else{
        if (implement_type == 0) {
            //two sided
            myBW = upper_power_of_two(myheight * 8);
#ifdef DEBUG_2
            cout << "commu_type=" << commu_type << ", implement_type=" << implement_type  << ", myBW="<<  myBW <<endl;
            cout.flush();
#endif
        } else if (implement_type == 1) { // fompi_put
            myBW = fompiput_upper_power_of_two(myheight * 8);
#ifdef DEBUG_2
            cout << "commu_type=" << commu_type << ", implement_type=" << implement_type  << ", myBW="<<  myBW <<endl;
            cout.flush();
#endif
        } else if (implement_type == 2) { // fompi_get
            myBW = fompiget_upper_power_of_two(myheight * 8);
#ifdef DEBUG_2
            cout << "commu_type=" << commu_type << ", implement_type=" << implement_type  << ", myBW="<<  myBW <<endl;
            cout.flush();
#endif
        }

        if (NPCOL >= 8) {
            time += log2(msgcnt) * ceil( NETWORK_LAT +  myheight * 8 / myBW);
#ifdef DEBUG_2
            cout << "(NPCOL>=8) time=" << time << endl;
            cout.flush();
#endif
        } else {
            time += msgcnt * ceil( NETWORK_LAT +  myheight * 8 / myBW);
#ifdef DEBUG_2
            cout << "(NPCOL<8) time=" << time << endl;
            cout.flush();
#endif
        }

    }
    return time;
}

vector<int> supernode;
void find_supernode(int i) {
    int j = i - 1;
    while (j >= 0) {
        if (exist_in_map(graph, i, j)) {
            break;
        } else {
            if (j == 0) {
                // super node shares a mutex with path
                // they will not use the mutex at the same time
                path_mutex->lock();
                supernode.push_back(i);
                path_mutex->unlock();
            }
            j--;
        }
    }
}

int main(int argc, char *argv[]) {
    int count, i, j, k;
    int n_node = 0;
    int sup_idx = 0;
    int start_point;
    int idx;
    int rankid, width,height;
    int maxwidth = 0, maxrank = 0; // maxcol = 0;
    int l;
    int index = 0; // index for critical path
    int size1; //=path.size();
    int modeltime=0;
    int col, row;
    int cur_col,cur_row;
    path_mutex = std::make_unique<std::mutex>();
    ThreadPool pool(NUM_OF_THREADS);

    maxpathlength=0;
    unordered_map<int, unordered_map<int, int>> myrank;
    unordered_map<int, unordered_map<int, int>> mywidth;
    unordered_map<int, unordered_map<int, int>> myheight;

    char* filename=argv[1];
    NPROW=atoi(argv[2]);
    NPCOL=atoi(argv[3]);

    FILE* fp = fopen(filename,"r");
    if (fp == NULL) {
	    fprintf(stderr, "Error reading file\n");
	    return 1;
    }
#ifdef DEBUG_0
    printf("Reading the file\n");
    fflush(stdout);
#endif
    count = 0;
    double totalsize=0;
    while (fscanf(fp, "%d,%d,%d,%d,%d", &col, &row, &rankid,&width,&height ) == 5) {
        graph[row][col] = 1;
        mylevel[row][col] = 0;
        mylevelMutexs[row][col] = std::make_unique<std::mutex>();
        //myrank[row][col] = rankid;
        mywidth[row][col] = width;
	    myheight[row][col] = height;
        maxcol = max(col, maxcol);
        totalsize += width * height;
        //maxwidth = max(width, maxwidth);
        //maxrank = max(rankid, maxrank);
        count++;
     }
    fclose(fp);

#ifdef DEBUG_0
    printf("End reading the file\n");
    fflush(stdout);
#endif

    supernode.push_back(0);
    mylevel[0][0] = 0;
    mylevelMutexs[0][0] = std::make_unique<std::mutex>();
    l = 0;
    // find all start rows
    for (i = 1; i <= maxcol; i++) {
        pool.enqueue(&find_supernode, i);
    }
    while (true) {
        cout << "Finding super nodes. Waiting nodes: "
             << pool.waiting_task_count() << endl;
        if (pool.waiting_task_count() == 0) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }

#ifdef DEBUG_0
    printf("Matrix blk size = %d x %d, total entries = %d, "
           "search path for every start point\n", maxcol, maxcol, sup_idx);
    fflush(stdout);
#endif
    for (auto super_index : supernode) {
        Cell start(super_index, super_index);
        vector<Cell> local_path;
        pool.enqueue(&find_path, start, local_path, l);
        //find_path(start, path, local_path, l);
    }

    while (true) {
        cout << "Waiting " << pool.waiting_task_count() << " task finish..." << endl;
        if (pool.waiting_task_count() == 0) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
    cout << "All tasks finished" << endl;
    cout  << "Current max path = " << maxpathlength << endl;

#ifdef DEBUG_1
    printf("END find path!!!!\n");
    fflush(stdout);
#endif

    /* Process Mapping */
    myrank[0][0]=0;
    for (i = 0; i <=maxcol; i++) {
 	    for (j = 0; j <=i; j++) {
     	    myrank[i][j]=(i%NPROW)*NPCOL+j%NPCOL;
     	    //cout << "row " << "col " << "Rank "<< endl;
            //cout << i+1 << " " << j+1 << " " << myrank[i][j]+1 << ".000000e+00"<< endl;
        }
    }

    int mmax=0;
    for (int i = 0; i < path.size(); i++) {
        if (path[i].size() > mmax) {
            mmax = path[i].size();
            index = i;
        }
    }
#ifdef DEBUG_0
    cout << "Critical path length " << path[index].size() <<  endl;
    cout.flush();
#endif

    idx = 0;
    while (idx < path[index].size()){
    	cur_col=path[index][idx].col;
        cur_row=path[index][idx].row;
#ifdef DEBUG_1
        cout << cur_row << "," << cur_col << "," << myrank[cur_row][cur_col] << "," << mylevel[cur_row][cur_col] << endl;
        cout.flush();
#endif
        idx += 1;
    }

    for (i = 0; i < sup_idx; i++) {
            Cell start(supernode[i], supernode[i]);
            find_level(start,l);
    }



/* LOWER BOUND */
    vector<int> lowbound(NPROW*NPCOL, 0);
    int r;
    for (i = 0; i < maxcol; i++) {
        j = 0;
        while (j <= i) {
            if (graph[i][j] == 1) { //&& myrank[i][j]!=myrank[i][i]) {
                r = myrank[i][j];
                lowbound[r] += ceil(mywidth[i][j] * myheight[i][j] * 8 / BW_CORE) + LAT_CORE;
            }
            j += 1;
        }
    }

    int lowbound_final=0;
    for (i=0; i<NPROW*NPCOL; i++){
        if(lowbound[i] > lowbound_final) lowbound_final = lowbound[i];
    }



    size1=path[index].size();
    vector<vector<int>> levelsize(size1, vector<int>(NPCOL*NPROW, 0));
    vector<vector<int>> leveltotbytes(size1, vector<int>(NPCOL*NPROW, 0));
    vector<float> levelGEMMtime(size1, 0);
    vector<int> levelGEMMrank(size1, 0);

#ifdef DEBUG_0
    cout << "Counting GEMM time" << endl;
    cout.flush();
#endif

    for (j = 0; j <=maxcol; j++) {
        for (l = 0; l <= maxcol; l++) {
            if (graph[j][l] == 1){
                levelsize[mylevel[j][l]][myrank[j][l]] += 1;
                leveltotbytes[mylevel[j][l]][myrank[j][l]] += mywidth[j][l] * (mywidth[j][l] > myheight[j][l] ? mywidth[j][l] : myheight[j][l]);
            }
        }
    }
    int tmp_max;
    for(i=0;i<size1;i++){
        tmp_max=0;
        for(j=0;j<NPCOL*NPROW;j++){
            if (leveltotbytes[i][j]> tmp_max) {
                tmp_max = leveltotbytes[i][j];
                levelGEMMrank[i] = j;
            }
        }
        levelGEMMtime[i] = tmp_max * 8 /BW_CORE + LAT_CORE;
    }

#ifdef DEBUG_0
    cout << "Counting COMM time" << endl;
    cout.flush();
#endif


    /* count out-degree   diag*/
    vector<int> sendoutmsg(maxcol, 0);
    int rootrank=0;
    int tmp_child=0;
    for (i = 0; i <= maxcol; i++) {
        j = i+1;
        rootrank=myrank[i][i];
        while (j <= maxcol){
            if (graph[j][i] == 1 && myrank[j][i] != rootrank) {
                sendoutmsg[i] += TREE_TYPE;
                tmp_child += 1;
            }
            if (tmp_child >= TREE_TYPE){
                rootrank = myrank[j][i];
                tmp_child = 0;
            }
            j += 1;
        }
    }
    /* count in-degree diag */
    vector<int> recvmsg(maxcol, 0);
    rootrank=0;
    tmp_child=0;
    for (i = 1; i < maxcol; i++) {
        j = 0;
        rootrank=myrank[i][i];
        while (j < i){
            if (graph[i][j] == 1 && myrank[i][j]!= rootrank) {
                recvmsg[i] += TREE_TYPE;
                tmp_child += 1;
            }
            if (tmp_child >= TREE_TYPE){
                rootrank = myrank[j][i];
                tmp_child = 0;
            }
            j += 1;
        }
    }

    vector<float> levelBCcommuTime_twoside(size1, 0);
    vector<float> levelRDcommuTime_twoside(size1, 0);
    vector<float> levelBCcommuTime_fompiput(size1, 0);
    vector<float> levelRDcommuTime_fompiput(size1, 0);
    vector<float> levelBCcommuTime_fompiget(size1, 0);
    vector<float> levelRDcommuTime_fompiget(size1, 0);

    for (j = 0; j < maxcol; j++) {
        /* model_message_time (int BC=0/RD=1), int twoside=0/fompiput=1/fompiget=2/nvshmemget=3, int mywidth, int myheight, int messagecnt)*/
        levelBCcommuTime_twoside[mylevel[j][j]] += model_message_time(0,0,mywidth[j][j],myheight[j][j],sendoutmsg[j]);
        levelRDcommuTime_twoside[mylevel[j][j]] += model_message_time(1,0,mywidth[j][j],myheight[j][j],recvmsg[j]);

        levelBCcommuTime_fompiput[mylevel[j][j]] += model_message_time(0,1,mywidth[j][j],myheight[j][j],sendoutmsg[j]);
        levelRDcommuTime_fompiput[mylevel[j][j]] += model_message_time(1,1,mywidth[j][j],myheight[j][j],recvmsg[j]);

        levelBCcommuTime_fompiget[mylevel[j][j]] += model_message_time(0,2,mywidth[j][j],myheight[j][j],sendoutmsg[j]);
        levelRDcommuTime_fompiget[mylevel[j][j]] += model_message_time(1,2,mywidth[j][j],myheight[j][j],recvmsg[j]);

    }


	i=index;

#ifdef DEBUG_0
 // print critial path
    float totalGEMMtime=0;
    float totalCommuTime_twoside=0;
    float totalCommuTime_fompiput=0;
    float totalCommuTime_fompiget=0;
    cout << "Critial path (" << path[index].size() << ") time on each level" << endl;
    cout << "level, rank, GEMMtime (ms,1e-6), Towsided (ms,1e-6),fompiput(ms,1e-6)"<< endl;
    cout.flush();
    int plevel;
    idx=0;
    while (idx < path[index].size()){
        totalGEMMtime += levelGEMMtime[idx];
        totalCommuTime_twoside += levelBCcommuTime_twoside[idx] + levelRDcommuTime_twoside[idx];
        totalCommuTime_fompiput += levelBCcommuTime_fompiput[idx] + levelRDcommuTime_fompiput[idx];
        totalCommuTime_fompiget += levelBCcommuTime_fompiget[idx] + levelRDcommuTime_fompiget[idx];


	    cout <<  idx  << " , " << levelGEMMrank[idx] << " , " << totalGEMMtime/1e3 << " , " << totalCommuTime_twoside/1e3 <<" , " << totalCommuTime_fompiput/1e3 << " , " << totalCommuTime_fompiget/1e3 << endl;
        cout.flush();
	    idx += 1;
    }
   cout << "level, rank, GEMMtime (ms,1e-6), Towsided (ms,1e-6),fompiput(ms,1e-6)"<< endl;
   cout << "Lower Bound (ms,1e-6) = " << lowbound_final/1e3 << endl;
   cout.flush();
#endif
		
       
return 0;
}
