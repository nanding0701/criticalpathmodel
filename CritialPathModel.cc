#include <stdio.h>
#include <iostream>
#include <stdlib.h>
#include <string.h>
#include <vector>
#include <math.h>

using namespace std;

#define SIZE 30000
#define BC_TREE_TYPE 0
#define BW_CORE 6  //6GB/s/core == 6B/ns
#define NETWORK_LAT 3810 // send 0 Byte
#define NPROW 2 
#define NPCOL 2 

//vector<unsigned long int> visited(SIZE, 0);
vector<vector<bool>> graph(SIZE, vector<bool>(SIZE, 0));
vector<vector<int>> mylevel(SIZE, vector<int>(SIZE, 0));

int maxcol;
int maxpathlength;

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
    void display() {
        cout << row << " " << col << endl;
    }
};

void display(bool a[][SIZE], int count) {
    for (int i = 0; i < count; i++) {
        for(int j = 0; j < count; j++) {
            printf("%d", a[i][j]);
        }
        printf("\n");
    }
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

void find_path(Cell point, vector<vector<Cell>>& path, vector<Cell> local_path, int l) {
#ifdef DEBUG
    cout << "I am here 1, point.col = " << point.col << ", point.row = " << point.row << endl;
    cout << " current total path num = " << path.size() << endl;
    cout.flush();
#endif
    local_path.push_back(point);
    l += 1;	
    if (point.col >= maxcol && point.col == point.row) {
#ifdef DEBUG
        cout << "find a path, size = " << local_path.size() << " current max = "<< maxpathlength << " current total path num = " <<  path.size() <<endl;
        cout.flush();
#endif
        if (maxpathlength < local_path.size()) {
            path.push_back(local_path);
            maxpathlength = local_path.size();
#ifdef DEBUG
            for (auto cell : local_path) {
                cell.display();
            }
            cout << endl;
#endif
        }else{
            local_path.clear();
        }

        return;
    }
        int cur_col = point.col;
        int row_end = point.row;
        int child_count = 0;
        vector<Cell> new_cells(SIZE);
        
        if (point.row == point.col) {
            row_end += 1;
#ifdef DEBUG
            cout << "I am here 2.1, this point row_end = " << row_end << endl;
            cout.flush();
#endif
		    while (row_end <= maxcol){
                if (graph[row_end][cur_col] == 1) {
                	if (mylevel[row_end][cur_col] < l){
                        mylevel[row_end][cur_col] = l;
	        	        new_cells[child_count].row = row_end;
                	    new_cells[child_count].col = cur_col;
                	    child_count++;
#ifdef DEBUG
            	        cout << "I am here 2.2, this point (" << point.row << ") find children (row col) = (" << row_end << "," << cur_col << ")" << endl;
                        cout.flush();
#endif
				    }
                }
            	row_end++;
       		}
#ifdef DEBUG
    		cout << "I am here 2.3, this point (" << point.row << ") find children total = " << child_count << endl;
            cout.flush();
#endif
            if (child_count == 0) {
#ifdef DEBUG
                cout << "find a path, size = " << local_path.size() << " current max = "<< maxpathlength << " current total path num = " <<  path.size() <<endl;
                cout.flush();
#endif
                if (maxpathlength < local_path.size()) {
                    path.push_back(local_path);
                    maxpathlength = local_path.size();
#ifdef DEBUG
                    for (auto cell : local_path) {
                        cell.display();
                    }
                    cout << endl;
#endif
                }else{
                    local_path.clear();
                }
            }
    		
            for (int i = 0; i < child_count; i++) {
#ifdef DEBUG
    		    cout << "I am here 2.4, start " << new_cells[i].row << " , " << new_cells[i].col << endl;
                cout.flush();
#endif
                find_path(new_cells[i], path, local_path, l);
#ifdef DEBUG
    		    cout << "I am here 2.4, end " << new_cells[i].row << " , " << new_cells[i].col << endl;
                cout.flush();
#endif
       		 }
	    }
    /* go to the end of current row */
    
   if (point.col < point.row) {
#ifdef DEBUG
    	cout << "I am here 2.5, this point = (" << point.col << " , " << point.row << " ), level = " << mylevel[point.row][point.row] <<endl;
        cout.flush();
#endif
       Cell new_point(point.row, point.row);
       if (mylevel[point.row][point.row] < l){
	        mylevel[point.row][point.row] = l;
       }
#ifdef DEBUG
    		cout << "I am here 2.6, start " << point.row << " , " << point.row << endl;
            cout.flush();
#endif
       find_path(new_point, path, local_path, l);
#ifdef DEBUG
    		cout << "I am here 2.6, end " << point.row << " , " << point.row << endl;
            cout.flush();
#endif
    }
#ifdef DEBUG
    cout << "I am here 2.6 point.col = " << point.col << ", point.row = " << point.row << endl;
    cout.flush();
#endif
    return;
}

float upper_power_of_two(int v)
{
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


float fompi_upper_power_of_two(int v)
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
    
    maxpathlength=0;
    vector<int> supernode(SIZE);
    vector<vector<int>> myrank(SIZE, vector<int>(SIZE));
    vector<vector<int>> mywidth(SIZE, vector<int>(SIZE));
    vector<vector<int>> myheight(SIZE, vector<int>(SIZE));
    vector<vector<int>> myblockN(SIZE, vector<int>(SIZE));
    
    //read matrix format: col#, row# 
    //FILE* fp = fopen("L_256ranks.csv","r");
    //FILE* fp = fopen("test.csv","r");
    char* filename=argv[1];
    FILE* fp = fopen(filename,"r");
    if (fp == NULL) {
	    fprintf(stderr, "Error reading file\n");
	    return 1;
    }
#ifdef DEBUG_1     
    printf("Reading the file\n");
    fflush(stdout);
#endif    
    count = 0;
    while (fscanf(fp, "%d,%d,%d,%d,%d", &col, &row, &rankid,&width,&height ) == 5) {
        graph[row][col] = 1;
        //myrank[row][col] = rankid;
        mywidth[row][col] = width;
	    myheight[row][col] = height; 
        maxcol = max(col, maxcol);
        //maxwidth = max(width, maxwidth);
        //maxrank = max(rankid, maxrank);
        count++;
     }
    fclose(fp);

#ifdef DEBUG_1     
    printf("End reading the file\n");
    fflush(stdout);
#endif    

    supernode[0] = 0;
    mylevel[0][0] = 0;
    sup_idx = 1;
    l=0;
    // find all start rows
    for (i = 1; i <= maxcol; i++) {
	    j = i - 1;
	    while (j >= 0) {
		    if (graph[i][j] == 1) {
			    break;
		    } else {
			    if (j == 0) {
				    supernode[sup_idx] = i;
				    sup_idx++;
				    mylevel[i][i]=l;
			    }
			    j--;
            }
	    }
    }
    
#ifdef DEBUG_1     
    printf("Matrix blk size = %d x %d, total entries = %d, search path for every start point\n", maxcol, maxcol, sup_idx);
    fflush(stdout);
#endif
    vector<vector<Cell>> path;
    for (i=0; i<sup_idx; i++){
        Cell start(supernode[i], supernode[i]);
        vector<Cell> local_path;
        find_path(start, path, local_path,l);
    }
#ifdef DEBUG_1     
    printf("END find path!!!!");
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
#ifdef DEBUG_1     
    cout << "Critical path length " << path[index].size() <<  endl;
    cout.flush();
#endif
    count=0;
    for (i = 0; i <= maxcol; i++) {
        for(j = 0; j <= i; j++){
  //  	    if (graph[i][j] == 1) {
    		    myblockN[i][j]=count;
  //  		    count++;
  //  	    }
        }
    }
    
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
            
//    /* count out-degree   diag*/
//    vector<int> sendoutmsg(maxcol+1, 0);
//    vector<int> sendoutmsgTime(maxcol+1, 0);
//    for (i = 0; i <= maxcol; i++) {
//	    j = i+1;
//	    while (j <= maxcol){
//		    if (graph[j][i] == 1 && myrank[j][i] != myrank[i][i]) {
//			    sendoutmsg[i] += 1;
//		    }
//		    j += 1;
//	    }
//    }
//
//    /* count in-degree diag */
//    int recv_rd_diag_rank[maxcol]; 
//    recv_rd_diag_rank[0]=0;
//    for (i = 1; i < maxcol; i++) {
//	    j = 0;
//	    recv_rd_diag_rank[i]=0;
//	    while (j < i){
//		    if (graph[i][j] == 1 && myrank[i][j]!=myrank[i][i]) {
//	            recv_rd_diag_rank[i] += 1;
//		    }
//	        j += 1;
//	    }
//	//cout << "row " << i << ", " <<recv_rd_diag[i] << endl;
//    }
//
//
//    vector<int> lowbound(NPROW*NPCOL, 0);
//    int r, r_new;
//    for (i = 0; i < maxcol; i++) {
//        j = 0;
//        while (j <= i){
//            if (graph[i][j] == 1){ //&& myrank[i][j]!=myrank[i][i]) {
//                r=myrank[i][j];
//                lowbound[r]+= ceil(mywidth[i][j]*myheight[i][j]*8/BW_CORE)+200;
//            } 
//	        j += 1;
//        }
//    }
//    int lowbound_final=0;
//    for (i=0; i<NPROW*NPCOL; i++){
//        if(lowbound[i] > lowbound_final) lowbound_final = lowbound[i];
//    }
//#ifdef DEBUG_1     
//    cout << "Lower Bound = " << lowbound_final << endl;
//    cout.flush();
//#endif   
//
//
//    size1=path[index].size();
//    vector<int> leveltime_0(size1+1, 0);
//    vector<int> depend_diag(size1+1,0); 
//    vector<int> fompi_depend_diag(size1+1,0); 
//    vector<int> fompicounter_depend_diag(size1+1,0); 
//    vector<int> levelsize(size1+1,0); 
//    modeltime=0;
//    int modeltime_0=0;
//    int modelMessagingtime=0;
//    int fompi_modelMessagingtime=0;
//    int fompicounter_modelMessagingtime=0;
//    bool visited;
//    int modelsize=0;
//#ifdef DEBUG_1     
//    cout << "Counting DGEMV time, Printing out dependency block (row,col,level)" << endl; 
//    cout.flush();
//#endif
//    for (i = 0; i < path[index].size(); i++) {
//    	    cur_col=path[index][i].col;
//            cur_row=path[index][i].row;
//            myblockN[cur_row][cur_col]=1;
//	        r=myrank[cur_row][cur_col];
//	        modelsize = max(mywidth[j][l],myheight[j][l]);
//		    modeltime_0 += ceil(modelsize * modelsize * 8 / BW_CORE) + 200;	
//	        if (cur_col==cur_row){
//                if ( NPROW >= 8){
//	    	        modelMessagingtime += NETWORK_LAT  + ceil ( log2(NPROW) * mywidth[cur_row][cur_row] *8 / upper_power_of_two(mywidth[cur_row][cur_row] *8)) + log2(NPCOL) * ceil (NETWORK_LAT + myheight[cur_row][cur_row]*8 / upper_power_of_two(myheight[cur_row][cur_row] *8 ));
//	    	        fompi_modelMessagingtime += NETWORK_LAT  + ceil ( log2(NPROW) * mywidth[cur_row][cur_row] *8 / fompi_upper_power_of_two(mywidth[cur_row][cur_row] *8)) + log2(NPCOL) * ceil (NETWORK_LAT + myheight[cur_row][cur_row]*8 / fompi_upper_power_of_two(myheight[cur_row][cur_row] *8 ));
//	    	        fompicounter_modelMessagingtime += NETWORK_LAT  + ceil ( log2(NPROW) * mywidth[cur_row][cur_row] *8 / fompicounter_upper_power_of_two(mywidth[cur_row][cur_row] *8)) + log2(NPCOL) * ceil (NETWORK_LAT + myheight[cur_row][cur_row]*8 / fompicounter_upper_power_of_two(myheight[cur_row][cur_row] *8 ));
//	            }else{
//	    	        modelMessagingtime += NETWORK_LAT  + ceil ( NPROW * mywidth[cur_row][cur_row] *8/ upper_power_of_two(mywidth[cur_row][cur_row] *8) ) + NPCOL * ceil (NETWORK_LAT + myheight[cur_row][cur_row] *8/ upper_power_of_two(myheight[cur_row][cur_row] *8) );
//	    	        fompi_modelMessagingtime += NETWORK_LAT  + ceil ( NPROW * mywidth[cur_row][cur_row] *8/ fompi_upper_power_of_two(mywidth[cur_row][cur_row] *8) ) + NPCOL * ceil (NETWORK_LAT + myheight[cur_row][cur_row] *8/ fompi_upper_power_of_two(myheight[cur_row][cur_row] *8) );
//	    	        fompicounter_modelMessagingtime += NETWORK_LAT  + ceil ( NPROW * mywidth[cur_row][cur_row] *8/ fompicounter_upper_power_of_two(mywidth[cur_row][cur_row] *8) ) + NPCOL * ceil (NETWORK_LAT + myheight[cur_row][cur_row] *8/ fompicounter_upper_power_of_two(myheight[cur_row][cur_row] *8) );
//	            } 
//	        }
//            
//            for (j = 0; j <=maxcol; j++){
//		        for (l = 0; l<=maxcol; l++) {
//                    if (myblockN[j][l]==1) continue;
//		            if (graph[j][l] == 1 && mylevel[j][l] == i){
//			            levelsize[i]++;
//                        myblockN[j][i]=1;
//		            }
//		            if (graph[j][l] == 1 && mylevel[j][l] <= i && myrank[j][l] == r ){
//#ifdef DEBUG_1     
//			            cout << "Path: " << j << "," << l << "," << r << ","<< i << endl; 
//                        cout.flush();
//#endif
//                        modelsize = max(mywidth[j][l],myheight[j][l]);
//			            modeltime_0 += ceil(modelsize * modelsize * 8 / BW_CORE) + 200;	
//                    }   
//                    if (graph[j][l] == 1 && mylevel[j][l] == i && myrank[j][l] == r && j==l){ 
//	    			        if ( NPROW >= 8){
//					            modelMessagingtime += NETWORK_LAT  + ceil ( log2(NPROW) * mywidth[cur_row][cur_row] * 8 / upper_power_of_two(mywidth[cur_row][cur_row] *8) ) +  ceil(log2(NPCOL)) * ceil (NETWORK_LAT + myheight[cur_row][cur_row] * 8 / upper_power_of_two(myheight[cur_row][cur_row] * 8) );
//					            fompi_modelMessagingtime += NETWORK_LAT  + ceil ( log2(NPROW) * mywidth[cur_row][cur_row] * 8 / fompi_upper_power_of_two(mywidth[cur_row][cur_row] *8) ) +  ceil(log2(NPCOL)) * ceil (NETWORK_LAT + myheight[cur_row][cur_row] * 8 / fompi_upper_power_of_two(myheight[cur_row][cur_row] * 8) );
//					            fompicounter_modelMessagingtime += NETWORK_LAT  + ceil ( log2(NPROW) * mywidth[cur_row][cur_row] * 8 / fompicounter_upper_power_of_two(mywidth[cur_row][cur_row] *8) ) +  ceil(log2(NPCOL)) * ceil (NETWORK_LAT + myheight[cur_row][cur_row] * 8 / fompicounter_upper_power_of_two(myheight[cur_row][cur_row] * 8) );
//				            }else{
//	    				        modelMessagingtime += NETWORK_LAT  + ceil ( NPROW * mywidth[cur_row][cur_row] * 8 / upper_power_of_two(mywidth[cur_row][cur_row] *8) ) + NPCOL * ceil (NETWORK_LAT + myheight[cur_row][cur_row] * 8/ upper_power_of_two(myheight[cur_row][cur_row] *8 ));
//	    				        fompi_modelMessagingtime += NETWORK_LAT  + ceil ( NPROW * mywidth[cur_row][cur_row] * 8 / fompi_upper_power_of_two(mywidth[cur_row][cur_row] *8) ) + NPCOL * ceil (NETWORK_LAT + myheight[cur_row][cur_row] * 8/ fompi_upper_power_of_two(myheight[cur_row][cur_row] *8 ));
//	    				        fompicounter_modelMessagingtime += NETWORK_LAT  + ceil ( NPROW * mywidth[cur_row][cur_row] * 8 / fompicounter_upper_power_of_two(mywidth[cur_row][cur_row] *8) ) + NPCOL * ceil (NETWORK_LAT + myheight[cur_row][cur_row] * 8/ fompicounter_upper_power_of_two(myheight[cur_row][cur_row] *8 ));
//				            }
//		    	    }
//		        }
//		    }
//	    
//	   leveltime_0[i] = modeltime_0 ; //ceil(mywidth[j][l]*myheight[j][l] * 8 / BW_CORE) + 200;
//	   depend_diag[i] = modelMessagingtime;
//	   fompi_depend_diag[i] = fompi_modelMessagingtime;
//	   fompicounter_depend_diag[i] = fompicounter_modelMessagingtime;
//    }
//	
//	i=index;
//
//#ifdef DEBUG_1     
// // print critial path
//   cout << " Critial path time on each level" << endl;
//   cout.flush();
//   int plevel;
//   index=0;
//   idx=0;
//   while (idx < path[index].size()){
//    	cur_col=path[index][idx].col;
//        cur_row=path[index][idx].row;
//   	    plevel = mylevel[cur_row][cur_col];    
//	    cout << plevel << " , " << myrank[cur_row][cur_col] << " , " << leveltime_0[plevel]/1e3 << " , " << depend_diag[plevel]/1e3 <<" , " << fompi_depend_diag[plevel]/1e3 << " , " << fompicounter_depend_diag[plevel] <<" , " << levelsize[plevel]<< endl; 	
//        cout.flush();
//	    idx += 1;
//  }
//    cout << "level, rank, dgemm,twoside,fompi,fompicounter,levelsize"<< endl;
//    cout.flush();
//#endif  
		
       
return 0;
}
