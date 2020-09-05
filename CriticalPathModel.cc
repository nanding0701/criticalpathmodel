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
#include <unordered_set>
#include <assert.h>
#include <algorithm>    // std::max


using namespace std;

#define TREE_TYPE 2
#define BW_CORE 6  //6GB/s/core == 6B/ns
#define LAT_CORE 200
//#define NETWORK_LAT 3810 // send 0 Byte ns
#define NUM_OF_THREADS 8 // The number of threads to find critical path
                         // One thread per core
unordered_map<int, unordered_map<int, bool>> graph;
unordered_map<int, unordered_map<int, int>> mylevel;
unordered_map<int, unordered_map<int, int>> myrank;
unordered_map<int, unordered_map<int, int>> mydep;
unordered_map<int, unordered_map<int, std::unique_ptr<std::mutex>>> mylevelMutexs;
std::unique_ptr<std::mutex> path_mutex;
unordered_map<int, unordered_map<int, int>> mywidth;
unordered_map<int, unordered_map<int, int>> myheight;


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


void find_level(Cell point, int l) {
    l += 1;

    if (point.col >= maxcol && point.col == point.row) {
        if (mylevel[point.col][point.col] < l){
            mylevelMutexs[point.col][point.col]->lock();
            mylevel[point.col][point.col] = l;
            mylevelMutexs[point.col][point.col]->unlock();
        }
        return;
    }


    int cur_col = point.col;
    int row_end = point.row;

    if (point.row == point.col) {
        row_end += 1;
        while (row_end <= maxcol){
            if (exist_in_map(graph, row_end, cur_col)){
                mylevelMutexs[row_end][cur_col]->lock();
                if (mylevel[row_end][cur_col] < l){
                    mylevel[row_end][cur_col] = l;
                }
                mylevelMutexs[row_end][cur_col]->unlock();
                Cell new_point(row_end, cur_col);
                find_level(new_point, l);
            }
            row_end++;
        }
    }


    if (point.row > point.col) {
        mylevelMutexs[point.row][cur_col]->lock();
        if (mylevel[point.row][cur_col] < l){
            mylevel[point.row][cur_col] = l;
        }
        mylevelMutexs[point.row][cur_col]->unlock();
        Cell new_point(point.row, point.row);
        find_level(new_point, l);
    }
    return;
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

void find_rank(int i){
    int j = 0;
    while (j <= i) {
        //for (j = 0; j <=i; j++) {
        if (exist_in_map(graph, i, j)) {
            myrank[i][j] = (i % NPROW) * NPCOL + j % NPCOL;
            //cout << "myrank " << myrank[i][j] << endl;
            //cout.flush();
        }
        j++;
    }
}

double upper_power_of_two(int v){
    //two-sided, 4node, roundtrip pingpong measurement, r/2 = one way bandwidth. on CORI KNL
    double network_bw[8]={0.020508, 0.033184 , 0.062388, 0.11532, 0.242152 ,0.651056, 1.078736, 1.677228};
    // msgsize=8,16,32,64,128,256,512,1024
    double v_new;
    v_new=pow(2, ceil(log(v)/log(2)));
    return network_bw[(int)log2(v_new)];
}
double fompiput_upper_power_of_two(int v)
{
    //one-sided, fompi_put, 4node, on CORI KNL
    double fompi[8]={0.032165,0.040741,0.080295,0.165825,0.32415,0.963243,2.466354, 5.875487};
    //double fompi[15]={5.19148,10.0762,21.0765,39.6508,83.754,149.342,279.648,414.555,881.995,1281.71,1736.03,2231.95,2430.68,2869.23,6234.01}; //haswell
    double v_new;
    v_new=pow(2, ceil(log(v)/log(2)));
    return fompi[(int)log2(v_new)];
}
double fompiget_upper_power_of_two(int v)
{
    //one-sided, fompi_put and fompi_get, mimic two-sided, 4node, on CORI KNL
    double fompi[8]={0.023657,0.040374,0.076177,0.137730,0.230902,0.444637,0.829184, 1.23984};
    //double fompi[15]={5.19148,10.0762,21.0765,39.6508,83.754,149.342,279.648,414.555,881.995,1281.71,1736.03,2231.95,2430.68,2869.23,6234.01}; //haswell
    double v_new;
    v_new=pow(2, ceil(log(v)/log(2)));
    return fompi[(int)log2(v_new)];
    //return v;
}
double fompicounter_upper_power_of_two(int v)
{
    double fompi_counter[15]={0.415952,0.728321,1.60955,3.11724,5.76481,12.0821,22.6828,49.2828,89.8562,165.587,335.04,674.643,1497.37,1993.54,2061.98};
    //double fompi_counter[15]={0.895065,1.58806,2.79401,6.21149,13.6465,26.819,47.7808,101.066,193.221,379.592,444.028,701.86,1421.58,2382.34,3986.8}; //haswell
    double v_new;
    v_new=pow(2, ceil(log(v)/log(2)));
    return fompi_counter[(int)log2(v_new)];
}

double model_message_time(int commu_type, int implement_type, int mywidth, int myheight, int msgcnt){
/* model_message_time (int BC=0/RD=1), int twoside=0/fompiput=1/fompiget=2/nvshmemget=3, int mywidth, int myheight, int messagecnt)*/
    double time=0.0;
    double myBW_bc=0.0, myBW_rd=0.0;

    double NETWORK_LAT;
    switch (implement_type) {
        case 0:
            NETWORK_LAT=11050.575972/1e9;
            myBW_bc = upper_power_of_two(mywidth);
            myBW_rd = upper_power_of_two(myheight);
            break;
        case 1:
            NETWORK_LAT=481.557846/1e9;
            myBW_bc = fompiput_upper_power_of_two(mywidth);
            myBW_rd = fompiput_upper_power_of_two(myheight);
            break;
        case 2:
            NETWORK_LAT=502.324104/1e9;
            myBW_bc = fompiget_upper_power_of_two(mywidth);
            myBW_rd = fompiget_upper_power_of_two(myheight);
            break;
    }



    switch(commu_type) {
        if (msgcnt==0) return 0;
        case 0:
            if (msgcnt==0) return 0;
            if (NPROW >= 8) {
                time = NETWORK_LAT + ceil(log2(msgcnt)) * mywidth * 8 / myBW_bc;
#ifdef DEBUG_2
                cout << "(NPROW>=8) time=" << time/1e9  << ",msgcnt=" << msgcnt << "/" << log2(msgcnt)<< endl;
                cout.flush();
#endif
            }else{
                time = NETWORK_LAT + ceil(msgcnt * mywidth * 8 / myBW_bc);
#ifdef DEBUG_2
                cout << "(NPROW<8) time=" << time << endl;
                cout.flush();
#endif
            }
            break;
        case 1:
            if (msgcnt==0) return 0;
            if (NPCOL >= 8) {
                time = ceil(log2(msgcnt)) *  NETWORK_LAT +  myheight * 8 / myBW_rd;
#ifdef DEBUG_2
                cout << "(NPCOL>=8) time=" << time/1e9 << ",msgcnt=" << msgcnt << "/" << log2(msgcnt)<< endl;
                cout.flush();
#endif
            } else {
                time = msgcnt * ceil(NETWORK_LAT + myheight * 8 / myBW_rd);
#ifdef DEBUG_2
                cout << "(NPCOL<8) time=" << time << endl;
                cout.flush();
#endif
            }
            break;

        case 2:
            if (msgcnt==0) return 0;
            time = msgcnt * mywidth * 8 / myBW_bc;
            break;
}

return time/1e9;
}


vector<double> lowbound_p;
vector<double> lowbound;
//vector<int,vector<std::unique_ptr<std::mutex>>> lowboundMutexs;
unordered_map<int, unordered_set<int>> levelranknum;
void count_lowerbound(int i) {
    int j = 0, l = 0, r = 0;
    while (j <= i) {
        if (exist_in_map(graph, i, j)) {
            l = mylevel[i][j];
            r = myrank[i][j];
            //lowboundMutexs[l][r]->lock();
            levelranknum[l].insert(r);
            //cout << "lower bound computing" << endl;
            lowbound[l] += ((mywidth[i][j] > myheight[i][j] ? mywidth[i][j] : myheight[i][j])
                            * mywidth[i][j] * 8 / BW_CORE) + LAT_CORE;
            //cout << "lower bound compute done" << endl;
            //lowboundMutexs[l][r]->unlock();
        }
        j++;
    }
}


void count_lowerbound_p(int i){
    int j = 0, l = 0, r = 0;
    while (j <= i) {
        if (exist_in_map(graph, i, j)) {
            r = myrank[i][j];
            //cout << "lower bound computing" << endl;
            lowbound_p[r] += ((mywidth[i][j] > myheight[i][j] ? mywidth[i][j] : myheight[i][j])
                            * mywidth[i][j] * 8 / BW_CORE) + LAT_CORE;
            //cout << "lower bound compute done" << lowbound_p[r] << endl;
            //cout.flush();
        }
        j++;
    }

}


vector<vector<double>> leveltotbytes;
unordered_map<int, unordered_map<int, std::unique_ptr<std::mutex>>> leveltotbytesMutexs;
void count_levelGEMM(int i){
    int l, r,j;
    for (j = 0; j<= maxcol; j++) {
        if (exist_in_map(graph, i, j)){
            l=mylevel[i][j];
            r=myrank[i][j];
            leveltotbytesMutexs[l][r]->lock();
            leveltotbytes[l][r] += mywidth[i][j] *
                                   (mywidth[i][j] > myheight[i][j] ? mywidth[i][j] : myheight[i][j]);
            leveltotbytesMutexs[l][r]->unlock();
        }
    }
}

void wait_pool_finish(ThreadPool& pool, std::string task_name, uint64_t task_count) {
    while (true) {
#ifdef DEBUG_0
        cout << task_name << ": waiting "
             << task_count - pool.get_finish_tasks() << " tasks " << endl;
        cout.flush();
#endif
        if (pool.waiting_task_count() == 0 && pool.get_finish_tasks() == task_count) {
            pool.reset_finish_task();
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
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

    char* filename=argv[1];
    NPROW=atoi(argv[2]);
    NPCOL=atoi(argv[3]);

    FILE* fp = fopen(filename,"r");
    if (fp == NULL) {
	    fprintf(stderr, "Error reading file\n");
	    return 1;
    }
#ifdef DEBUG_0
    printf("NPROW=%d, NPCOL=%d\n",NPROW,NPCOL);
    printf("Reading the file\n");
    fflush(stdout);
#endif
    count = 0;
    double totalsize=0;
    while (fscanf(fp, "%d,%d,%d,%d,%d", &col, &row, &rankid,&width,&height ) == 5) {
        graph[row][col] = 1;
        mylevel[row][col] = 0;
        mylevelMutexs[row][col] = std::make_unique<std::mutex>();
        myrank[row][col] = 0;
        mydep[row][col] = 0;
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

    wait_pool_finish(pool, "Finding super node", maxcol);

#ifdef DEBUG_0
    cout << "Matrix blk size = " << maxcol << " x " << maxcol
         << ", total entries = " << supernode.size()
         << " search path for every start point" << endl;
    cout.flush();
#endif
    for (auto super_index : supernode) {
        Cell start(super_index, super_index);
        vector<Cell> local_path;
        pool.enqueue(&find_path, start, local_path, l);
        //find_path(start, path, local_path, l);
    }

    wait_pool_finish(pool, "Finding Path", supernode.size());

#ifdef DEBUG_0
    cout << "All tasks finished" << endl;
    cout  << "path length= " << maxpathlength << endl;
    cout.flush();
#endif

#ifdef DEBUG_0
    cout << "Counting levels" << endl;
    cout.flush();
#endif
    for (auto super_index : supernode) {
        l=0;
        Cell start(super_index, super_index);
        pool.enqueue(&find_level, start, l);
    }

    wait_pool_finish(pool, "Finding Levels", supernode.size());

#ifdef DEBUG_0
    cout << "All levels finished" << endl;
    cout.flush();
#endif


#ifdef DEBUG_0
    printf("Process Decomposition....\n");
    fflush(stdout);
#endif
    /* Process Mapping */
    myrank[0][0] = 0;
    for (i = 0; i <= maxcol; i++) {
        pool.enqueue(&find_rank, i);
    }

    wait_pool_finish(pool, "Finding Rank", maxcol + 1);

#ifdef DEBUG_1
    for (i = 0; i <=maxcol; i++) {
        j=0;
        while (j<=i){
            if (exist_in_map(graph, i, j)) {
                cout << i << j << myrank[i][j] << mylevel[i][j]<< endl;
                cout.flush();
            }
            j++;
        }
    }
#endif

    int mmax = 0;

    for (int i = 0; i < path.size(); i++) {
        if (path[i].size() > mmax) {
            mmax = path[i].size();
            index = i;
        }
    }
#ifdef DEBUG_1
    idx = 0;
    while (idx < path[index].size()){
    	cur_col=path[index][idx].col;
        cur_row=path[index][idx].row;

        cout << cur_row << "," << cur_col << "," << myrank[cur_row][cur_col] << "," << mylevel[cur_row][cur_col] << endl;
        cout.flush();

        idx += 1;
    }
#endif

#ifdef DEBUG_0
    cout << "Counting degree time" << endl;
    cout.flush();
#endif


    /* count out-degree   diag*/
    vector<int> sendoutmsg(maxcol, 0);
    int rootrank=0;
    for (i = 0; i <= maxcol; i++) {
        j = i+1;
        rootrank=myrank[i][i];
        while (j <= maxcol){
            if (graph[j][i] == 1 && myrank[j][i] != rootrank) {
                sendoutmsg[i] += 1;
                mydep[j][i] +=1;
            }
            j += 1;
        }
    }
    /* count in-degree diag */
    vector<int> recvmsg(maxcol, 0);
    rootrank=0;
    for (i = 1; i < maxcol; i++) {
        j = 0;
        rootrank=myrank[i][i];
        while (j < i){
            if (graph[i][j] == 1 && myrank[i][j]!= rootrank) {
                recvmsg[i] += 1;
            }
            j += 1;
        }
    }


#ifdef DEBUG_0
    cout << "Counting LOWER BOUND" << endl;
    cout.flush();
#endif


    lowbound_p.resize(NPCOL*NPROW, 0);

    //cout << "init lowbound_p" << endl;
    size1=path[index].size();
    lowbound.resize(size1,0);
    //cout << "init lowbound" << endl;

    for (i = 0; i <= maxcol; i++) {
        count_lowerbound_p(i);
        //pool.enqueue(&count_lowerbound, i);
    }
    for (i = 0; i <= maxcol; i++) {
        count_lowerbound(i);
        //pool.enqueue(&count_lowerbound, i);
    }

#ifdef DEBUG_0
    cout << "Wait for Counting LOWER BOUND" << endl;
    cout.flush();
#endif
    double tmp_max_1=0;
    double lowbound_final=0;
    //perfect parallel
    for (i=0; i<path[index].size(); i++){
        lowbound_final += ceil(lowbound[i]/levelranknum[i].size());
    }
    //Pmax GEMM
    double lowbound_final_p=0;
    for (j=0; j<NPROW*NPCOL; j++) {
        if(lowbound_p[i]>lowbound_final_p ) lowbound_final_p=lowbound_p[i];
    }


#ifdef DEBUG_0
    cout << "Counting GEMM time" << endl;
    cout.flush();
#endif
    leveltotbytes.resize(size1, vector<double>(NPCOL*NPROW, 0));
    for(i=0;i<path[index].size();i++) {
        for (j = 0; j < NPROW * NPCOL; j++) {
            leveltotbytesMutexs[i][j] = std::make_unique<std::mutex>();
        }
    }

    for (i = 0; i <= maxcol; i++) {
        pool.enqueue(&count_levelGEMM, i);
    }

    wait_pool_finish(pool, "Counting Level GEMM", maxcol + 1);

    vector<double> levelGEMMtime(size1, 0);
    vector<int> levelGEMMrank(size1, 0);
    vector<vector<double>> leveltime_perrank(size1,vector<double>(NPCOL*NPROW,0.0));
    int tmp_max;
    for(i=0;i<size1;i++){
        tmp_max=0;
        for(j=0;j<NPCOL*NPROW;j++){
            leveltime_perrank[i][j]+=leveltotbytes[i][j] * 8/BW_CORE + LAT_CORE;
            if (leveltotbytes[i][j]> tmp_max) {
                tmp_max = leveltotbytes[i][j];
                levelGEMMrank[i] = j;
            }
        }
        levelGEMMtime[i] = tmp_max * 8 /BW_CORE + LAT_CORE;
    }

    //vector<double> totaltime_p_withdep(NPROW*NPCOL, 0);;
    //for(i=0; i<NPCOL*NPROW;i++){
    //    totaltime_p_withdep[i]+=lowbound_p[i]/1e9;
    //}



#ifdef DEBUG_0
    cout << "Counting COMM time" << endl;
    cout.flush();
#endif
    vector<double> levelBCcommuTime_twoside(size1, 0);
    vector<double> levelRDcommuTime_twoside(size1, 0);
    vector<double> levelBCcommuTime_fompiput(size1, 0);
    vector<double> levelRDcommuTime_fompiput(size1, 0);
    vector<double> levelBCcommuTime_fompiget(size1, 0);
    vector<double> levelRDcommuTime_fompiget(size1, 0);

    unordered_map<int, unordered_map<int, double>> levelrank_BCcommuTime_fompiget;
    unordered_map<int, unordered_map<int, double>> levelrank_RDcommuTime_fompiget;
    for (j = 0; j < maxcol; j++) {
        /* model_message_time (int BC=0/RD=1), int twoside=0/fompiput=1/fompiget=2/nvshmemget=3, int mywidth, int myheight, int messagecnt)*/
/*
        // upper bound, no overlap, as observed on the criticalpath
        levelBCcommuTime_twoside[mylevel[j][j]] += model_message_time(0,0,mywidth[j][j],myheight[j][j],sendoutmsg[j]);
        levelRDcommuTime_twoside[mylevel[j][j]] += model_message_time(1,0,mywidth[j][j],myheight[j][j],recvmsg[j]);
        //levelBCcommuTime_twoside[mylevel[j][j]] += model_message_time(0,0,mywidth[j][j],myheight[j][j], NPROW);
        //levelRDcommuTime_twoside[mylevel[j][j]] += model_message_time(1,0,mywidth[j][j],myheight[j][j], NPCOL);
        levelBCcommuTime_fompiput[mylevel[j][j]] += model_message_time(0,1,mywidth[j][j],myheight[j][j],sendoutmsg[j]);
        levelRDcommuTime_fompiput[mylevel[j][j]] += model_message_time(1,1,mywidth[j][j],myheight[j][j],recvmsg[j]);
*/
        levelBCcommuTime_fompiget[mylevel[j][j]] += model_message_time(0,2,mywidth[j][j],myheight[j][j],sendoutmsg[j]);
        levelRDcommuTime_fompiget[mylevel[j][j]] += model_message_time(1,2,mywidth[j][j],myheight[j][j],recvmsg[j]);

        // overlaped within levels
        levelrank_BCcommuTime_fompiget[mylevel[j][j]][myrank[j][j]] += model_message_time(0,2,mywidth[j][j],myheight[j][j],sendoutmsg[j]);
        levelrank_RDcommuTime_fompiget[mylevel[j][j]][myrank[j][j]] += model_message_time(1,2,mywidth[j][j],myheight[j][j],recvmsg[j]);

        //totaltime_p_withdep[myrank[j][j]] += model_message_time(1,2,mywidth[j][j],myheight[j][j],recvmsg[j]);
    }
    //for (i = 0; i < maxcol; i++) {
    //    for (j = 0; j<= maxcol; j++) {
    //        if (exist_in_map(graph, i, j) && mydep[i][j]!=0){
    //            totaltime_p_withdep[myrank[i][j]] += model_message_time(3,2,mywidth[j][j],myheight[j][j],mydep[i][j]); //3 recv BC time
    //        }
    //    }
    //}



    double totalGEMMtime=0;
    double totalCommuTime_twoside=0;
    double totalCommuTime_fompiput=0;
    double totalCommuTime_fompiget=0;
    int plevel;
    idx=0;

//#ifdef DEBUG_0
//    cout << "Critial path (" << path[index].size() << ") time on each level" << endl;
//    cout << "level, rank, size,  Towsided(s),fompiput(s), fompiget(s)"<< endl;
//    cout.flush();
//#endif
#ifdef DEBUG_0
    cout << "Counting total time" << endl;
    cout.flush();
#endif



    while (idx < path[index].size()){
//        totalGEMMtime += levelGEMMtime[idx];
//        totalCommuTime_twoside += levelBCcommuTime_twoside[idx] + levelRDcommuTime_twoside[idx];
//        totalCommuTime_fompiput += levelBCcommuTime_fompiput[idx] + levelRDcommuTime_fompiput[idx];
        totalCommuTime_fompiget += levelBCcommuTime_fompiget[idx] + levelRDcommuTime_fompiget[idx];
//#ifdef DEBUG_0
//	    cout <<  idx  << " , " << levelGEMMrank[idx] << " , " << levelranknum[idx].size() << " , "   << totalCommuTime_twoside <<" , " << totalCommuTime_fompiput << " , " << totalCommuTime_fompiget << endl;
//        cout.flush();
//#endif
	    idx += 1;
    }
//#ifdef DEBUG_0
//   cout << "level, rank, size, Twosided(s),fompiput(s), fompiget(s)"<< endl;
//   cout.flush();
//#endif
//   cout << "totalCommuTime_twoside(s): " << totalCommuTime_twoside << "totalCommuTime_fompiput(s): " << totalCommuTime_fompiput << ", totalCommuTime_fompiget(s): " << totalCommuTime_fompiget << endl;

//   cout << " GEMV p_max: " << lowbound_final_p/1e9 <<  ", lowbound(s): " << lowbound_final/1e9 << ", upper(ns): " << totalGEMMtime/1e9 << endl;
//   cout.flush();

   double overlap_totaltime=0;
   double tmp_sum;
   vector<double> level_maxtime(NPCOL*NPROW, 0);
   idx =0 ;
   while (idx < path[index].size()){
       //tmp_sum=0;
       for(j=0;j<NPCOL*NPROW;j++) {
           level_maxtime[j]=max(leveltime_perrank[idx][j]/1e9, levelrank_BCcommuTime_fompiget[idx][j]+levelrank_RDcommuTime_fompiget[idx][j]);
       }
       overlap_totaltime += *max_element(level_maxtime.begin(), level_maxtime.end());
       idx += 1;
   }



   cout << " No overlap time:" << lowbound_final_p/1e9+totalCommuTime_fompiget  << ", GEMV time:" << lowbound_final_p/1e9 << ", COMM time:" << totalCommuTime_fompiget << endl;
   cout << " Overlap totaltime:" << overlap_totaltime<< endl;
   //cout << " Pmax totaltime:" << *max_element(totaltime_p_withdep.begin(), totaltime_p_withdep.end()) << endl;
   cout.flush();
		
       
return 0;
}
