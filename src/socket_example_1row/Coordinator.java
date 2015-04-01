package socket_example_1row;

import java.io.*;  

import matrix.*; 

public class Coordinator { 
	
	Connection conn; 
	int dim; 
	 int[][] a; 
	 int[][] b; 
	int[][] c; 
	int numNodes; 
	DataInputStream[] disWorkers;
	DataOutputStream[] dosWorkers; 
	
	public Coordinator(int n, int numNodes) { 
		this.dim = n; 
		a = new int[n][n]; 
		b = new int[n][n]; 
		c = new int[n][n]; 
		this.numNodes = numNodes; 
	}
	
 	void configurate(int portNum) { 
		try { 
			conn = new Connection(portNum); 
			disWorkers = new DataInputStream[numNodes]; 
			dosWorkers = new DataOutputStream[numNodes];
			String[] ips = new String[numNodes]; 
			int[] ports = new int[numNodes]; 
			for (int i=0; i<numNodes; i++ ) { 
				DataIO dio = conn.acceptConnect(); 
				DataInputStream dis = dio.getDis(); 
				int nodeNum = dis.readInt(); 			//get worker ID
				ips[nodeNum] = dis.readUTF(); 			//get worker ip
				ports[nodeNum] = dis.readInt();  		//get worker port #
				disWorkers[nodeNum] = dis; 
				dosWorkers[nodeNum] = dio.getDos(); 	//the stream to worker ID
				dosWorkers[nodeNum].writeInt(dim/2); 		//assign matrix dimension (height)
				int width = dim/2/*(nodeNum<numNodes-1) ? dim/numNodes : dim/numNodes+dim%numNodes*/;
				dosWorkers[nodeNum].writeInt(width); 	//assign matrix width 
			} 
			
			//left shift ip and ports
			dosWorkers[0].writeUTF(ips[1]);
			dosWorkers[0].writeInt(ports[1]);
			dosWorkers[0].writeUTF(ips[1]);
			dosWorkers[0].writeInt(ports[1]);
			dosWorkers[1].writeUTF(ips[0]);
			dosWorkers[1].writeInt(ports[0]);
			dosWorkers[1].writeUTF(ips[0]);
			dosWorkers[1].writeInt(ports[0]);
			
			dosWorkers[2].writeUTF(ips[3]);
			dosWorkers[2].writeInt(ports[3]);
			dosWorkers[2].writeUTF(ips[3]);
			dosWorkers[2].writeInt(ports[3]);
			dosWorkers[3].writeUTF(ips[2]);
			dosWorkers[3].writeInt(ports[2]);
			dosWorkers[3].writeUTF(ips[2]);
			dosWorkers[3].writeInt(ports[2]);
			
			//top shift ip and ports
			
			dosWorkers[0].writeUTF(ips[2]);
			dosWorkers[0].writeInt(ports[2]);
			dosWorkers[0].writeUTF(ips[2]);
			dosWorkers[0].writeInt(ports[2]);
			dosWorkers[1].writeUTF(ips[3]);
			dosWorkers[1].writeInt(ports[3]);
			dosWorkers[1].writeUTF(ips[3]);
			dosWorkers[1].writeInt(ports[3]);
			
			dosWorkers[2].writeUTF(ips[0]);
			dosWorkers[2].writeInt(ports[0]);
			dosWorkers[2].writeUTF(ips[0]);
			dosWorkers[2].writeInt(ports[0]);
			dosWorkers[3].writeUTF(ips[1]);
			dosWorkers[3].writeInt(ports[1]);
			dosWorkers[3].writeUTF(ips[1]);
			dosWorkers[3].writeInt(ports[1]);
	
		} catch (IOException ioe) { 
			System.out.println("error: Coordinator assigning neighbor infor.");  
			ioe.printStackTrace(); 
		} 
	}	
 	
 	public static int[][] hShiftMatrix(int[][]a, int n)
	{
		//Code shifts matrix horizontally -- first row by 1 shift, second row by 2 shifts and so on
		int i, j;
		int k=0;
		int temp = 0;
		
		while(k < n )
		{
		for(i = k; i < n; i++)
		{			
			temp = a[i][0];			
			for(j = 0; j < n-1; j++)				
			{											
				a[i][j] = a[i][j+1];			
			}			
			a[i][n-1] = temp;
			}
		k++;
		}		
		return a;
	}
	public static int[][] vShiftMatrix(int[][]a, int n)
	{
		//Code shifts matrix vertically -- first row by 1 shift, second row by 2 shifts and so on
		int i, j;
		int k=0;
		int temp = 0;
		
		while(k < n )
		{
		for(j = k; j < n; j++)
		{			
			temp = a[0][j];			
			for(i = 0; i < n-1; i++)				
			{											
				a[i][j] = a[i+1][j];			
			}			
			a[n-1][j] = temp;
			}
		k++;
		}		
		return a;
	}

 	
	
	void distributeA(int numNodes) {
			
		for (int w = 0; w < numNodes; w++) {			// send blocks
			switch(w){			
			case 0: for(int i=0; i<2*dim/numNodes; i++){
				for(int j=0;j<2*dim/numNodes;j++)
				{
					try {
						dosWorkers[w].writeInt(a[i][j]);
						
					}
					catch (IOException ioe){
						System.out.println("error in distribute: " + i + ", " + j);
						ioe.printStackTrace();
					}System.out.println("Sent "+a[i][j]+" to Worker" + w); 
					}
				}
			break;
			case 1:for(int i=0; i<2*dim/numNodes; i++){
				for(int j=2*dim/numNodes;j<4*dim/numNodes;j++)
				{
					try {
						dosWorkers[w].writeInt(a[i][j]);
						
					}
					catch (IOException ioe){
						System.out.println("error in distribute: " + i + ", " + j);
						ioe.printStackTrace();
					}System.out.println("Sent "+a[i][j]+" to Worker" + w); 
					}
				}
			break;
			case 2:for(int i=2*dim/numNodes; i<4*dim/numNodes; i++){
				for(int j=0;j<2*dim/numNodes;j++)
				{
					try {
						dosWorkers[w].writeInt(a[i][j]);
						
					}
					catch (IOException ioe){
						System.out.println("error in distribute: " + i + ", " + j);
						ioe.printStackTrace();
					}System.out.println("Sent "+a[i][j]+" to Worker" + w); 
					}
				} 
			break;
			case 3:for(int i=2*dim/numNodes; i<4*dim/numNodes; i++){
				for(int j=2*dim/numNodes;j<4*dim/numNodes;j++)
				{
					try {
						dosWorkers[w].writeInt(a[i][j]);
						
					}
					catch (IOException ioe){
						System.out.println("error in distribute: " + i + ", " + j);
						ioe.printStackTrace();
						}System.out.println("Sent "+a[i][j]+" to Worker" + w); 
				} 
				}	
			break;
			}
		}
		}

		void distributeB(int numNodes) {
				
			for (int w = 0; w < numNodes; w++) {			// send blocks
				switch(w){			
				case 0: for(int i=0; i<2*dim/numNodes; i++){
					for(int j=0;j<2*dim/numNodes;j++)
					{
						try {
							dosWorkers[w].writeInt(b[i][j]);
							
						}
						catch (IOException ioe){
							System.out.println("error in distribute: " + i + ", " + j);
							ioe.printStackTrace();
						}System.out.println("Sent "+b[i][j]+" to Worker" + w); 
						}
					}
				break;
				case 1:for(int i=0; i<2*dim/numNodes; i++){
					for(int j=2*dim/numNodes;j<4*dim/numNodes;j++)
					{
						try {
							dosWorkers[w].writeInt(b[i][j]);
							
						}
						catch (IOException ioe){
							System.out.println("error in distribute: " + i + ", " + j);
							ioe.printStackTrace();
						}System.out.println("Sent "+b[i][j]+" to Worker" + w); 
						}
					}
				break;
				case 2:for(int i=2*dim/numNodes; i<4*dim/numNodes; i++){
					for(int j=0;j<2*dim/numNodes;j++)
					{
						try {
							dosWorkers[w].writeInt(b[i][j]);
							
						}
						catch (IOException ioe){
							System.out.println("error in distribute: " + i + ", " + j);
							ioe.printStackTrace();
						}System.out.println("Sent "+b[i][j]+" to Worker" + w); 
						}
					} 
				break;
				case 3:for(int i=2*dim/numNodes; i<4*dim/numNodes; i++){
					for(int j=2*dim/numNodes;j<4*dim/numNodes;j++)
					{
						try {
							dosWorkers[w].writeInt(b[i][j]);
							
						}
						catch (IOException ioe){
							System.out.println("error in distribute: " + i + ", " + j);
							ioe.printStackTrace();
							}System.out.println("Sent "+b[i][j]+" to Worker" + w); 
					} 
					}	
				break;
				}
			}

			
			
		
		
		
		
		
		/*for (int w = 0; w < numNodes; w++) {			// send blocks 
			int width = (w<numNodes-1) ? (w+1)*dim/numNodes : dim; 
			for (int i = 0; i < dim; i++) { 
				for (int j = w*dim/numNodes; j < width; j++) { 
					try { 
						dosWorkers[w].writeInt(a[i][j]); 
					} catch (IOException ioe) { 
						System.out.println("error in distribute: " + i + ", " + j);  
						ioe.printStackTrace(); 
					}
				} 
			} 
			System.out.println("Sent to Worker" + w); 
		} */
	}
	
	void Display() {
		a = MatrixMultiple.createDisplayMatrix(dim); 
		System.out.println("MATRIX A");
		MatrixMultiple.displayMatrix(a); 
		
	    a= hShiftMatrix(a, numNodes);
	    
	    System.out.println("Initalized A matrix");
	    MatrixMultiple.displayMatrix(a);
	    	
		
		b=MatrixMultiple.createDisplayMatrix(dim);
		System.out.println("MATRIX B");
		MatrixMultiple.displayMatrix(b);
		
		   b= vShiftMatrix(b, numNodes);
		   System.out.println("Initalized B matrix");
		    MatrixMultiple.displayMatrix(b);
		    
		    
		
	}
	
	void DisplayC()
	{
		for (int w = 0; w < numNodes; w++) {			// send blocks
			switch(w){			
			case 0: for(int i=0; i<2*dim/numNodes; i++){
				for(int j=0;j<2*dim/numNodes;j++)
				{
					try {
						c[i][j] = disWorkers[w].readInt();
						
					}
					catch (IOException ioe){
						System.out.println("error in distribute: " + i + ", " + j);
						ioe.printStackTrace();
					}System.out.println("Sent "+b[i][j]+" to Worker" + w); 
					}
				}
			break;
			case 1:for(int i=0; i<2*dim/numNodes; i++){
				for(int j=2*dim/numNodes;j<4*dim/numNodes;j++)
				{
					try {
						c[i][j] = disWorkers[w].readInt();
						
					}
					catch (IOException ioe){
						System.out.println("error in distribute: " + i + ", " + j);
						ioe.printStackTrace();
					}System.out.println("Sent "+b[i][j]+" to Worker" + w); 
					}
				}
			break;
			case 2:for(int i=2*dim/numNodes; i<4*dim/numNodes; i++){
				for(int j=0;j<2*dim/numNodes;j++)
				{
					try {
						c[i][j] = disWorkers[w].readInt();
						
					}
					catch (IOException ioe){
						System.out.println("error in distribute: " + i + ", " + j);
						ioe.printStackTrace();
					}System.out.println("Sent "+b[i][j]+" to Worker" + w); 
					}
				} 
			break;
			case 3:for(int i=2*dim/numNodes; i<4*dim/numNodes; i++){
				for(int j=2*dim/numNodes;j<4*dim/numNodes;j++)
				{
					try {
						c[i][j] = disWorkers[w].readInt();
						
					}
					catch (IOException ioe){
						System.out.println("error in distribute: " + i + ", " + j);
						ioe.printStackTrace();
						}System.out.println("Sent "+b[i][j]+" to Worker" + w); 
				} 
				}	
			break;
			}}
		
		MatrixMultiple.displayMatrix(c,dim);
	}
	
	
	public static void main(String[] args) { 
		if (args.length != 3) {
			System.out.println("usage: java Coordinator maxtrix-dim number-nodes coordinator-port-num"); 
		} 
		int numNodes = Integer.parseInt(args[1]); 
		Coordinator coor = new Coordinator(Integer.parseInt(args[0]), numNodes); 
		coor.configurate(Integer.parseInt(args[2])); 		
		coor.Display();		
		coor.distributeA(numNodes); 
		coor.distributeB(numNodes);		
		coor.DisplayC();		
		try {Thread.sleep(12000);} catch (Exception e) {e.printStackTrace();}
		System.out.println("Done.");
	}
}
