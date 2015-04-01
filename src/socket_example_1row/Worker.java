package socket_example_1row;

import java.io.*;
import java.net.InetAddress;

import matrix.*;

public class Worker {

	int nodeNum;
	int localPort;
	Connection conn;
	int dim; 
	int width; 
	static int[][] a;
	static int[][] b;
	static int[][] c;
	DataInputStream disCoor;
	DataOutputStream dosCoor;
	DataOutputStream dosLeft;
	DataInputStream disRight;
	DataOutputStream dosUp;
	DataInputStream disDown;

	public Worker(int nodeNum, int localPort) {
		this.nodeNum = nodeNum;
		this.localPort = localPort;
	}

	void configurate(String coorIP, int coorPort) {
		try {
			conn = new Connection(localPort); 
			DataIO dio = conn.connectIO(coorIP, coorPort); 
			dosCoor = dio.getDos();  
			dosCoor.writeInt(nodeNum);
			dosCoor.writeUTF(InetAddress.getLocalHost().getHostAddress());
			dosCoor.writeInt(localPort);
			disCoor = dio.getDis();
			dim = disCoor.readInt(); 				//get matrix dimension from coordinator
			width = disCoor.readInt();
			a = new int[dim][width];
			b = new int[dim][width];
			c = new int[dim][width];
			String ipLeft = disCoor.readUTF();		//left block connection info
			int portLeft = disCoor.readInt();
			String ipRight = disCoor.readUTF();		//right block connection info 
			int portRight = disCoor.readInt();
			
			String ipUp = disCoor.readUTF();		//top block connection info
			int portUp = disCoor.readInt();
			String ipDown = disCoor.readUTF();		//bottom block connection info 
			int portDown = disCoor.readInt();
			
			dosUp = conn.connect2write(ipUp, portUp);
			disDown = conn.accept2read();	 
			
			
			if (nodeNum%2==0) {		// Even # worker connecting manner
				dosLeft = conn.connect2write(ipLeft, portLeft);
				disRight = conn.accept2read();	 
			} else {				// Odd # worker connecting manner
				disRight = conn.accept2read();  
				dosLeft = conn.connect2write(ipRight, portRight);
			}
		} catch (IOException ioe) {
			ioe.printStackTrace();
		} 
		System.out.println("Configuration done."); 
	}

	
	void calculate() {
		
		int k=0;
		while(k<2*dim)
		{
			for(int i=0;i<dim;i++)
			{
				for(int j=0;j<dim;j++)
				{
					c[i][j]=c[i][j]+a[i][j]*b[i][j];
				}
			}
			
			k++;
			compute();
			topShift();
			
		}
		
		
		System.out.println("Matrix C calculated");
		MatrixMultiple.displayMatrix(c);
		
		for(int i=0;i<dim;i++)
		{
			for(int j=0;j<dim;j++)
			{
				try {
					dosCoor.writeInt(c[i][j]);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
		
	   
	
	}
	void compute() {
		// get the block from coordinator 
		
		
		// shift matrix a toward left 
		int[] tempIn = new int[dim]; 
		int[] tempOut = new int[dim]; 
		if (nodeNum%2==0) { 		// Even # worker shifting procedure 
			for (int i = 0; i < dim; i++) {
				try {
					dosLeft.writeInt(a[i][0]);
				} catch (IOException ioe) {
					System.out.println("error in sending to left, row=" + i);
					ioe.printStackTrace();
				}
			}
			// local shift
			for (int i = 0; i < dim; i++) {		
				for (int j = 1; j < width; j++) {
					a[i][j-1] = a[i][j];
				}
			} 
			// receive the rightmost column
			for (int i = 0; i < dim; i++) {
				try {
 					a[i][width-1] = disRight.readInt();
				} catch (IOException ioe) {
					System.out.println("error in receiving from right, row=" + i);
					ioe.printStackTrace();
				}
			}
		} else { 					// Odd # worker shifting procedure
			for (int i = 0; i < dim; i++) {		// receive a column from right
				try {
					tempIn[i] = disRight.readInt();
				} catch (IOException ioe) {
					System.out.println("error in receiving from right, row=" + i);
					ioe.printStackTrace();
				}
			} 
			for (int i = 0; i < dim; i++) {		// local shift
				tempOut[i] = a[i][0];
			} 
			for (int i = 0; i < dim; i++) {		
				for (int j = 1; j < width; j++) {
					a[i][j-1] = a[i][j];
				}
			} 
			for (int i = 0; i < dim; i++) {		
				a[i][width-1] = tempIn[i];
			} 
			for (int i = 0; i < dim; i++) {		// send leftmost column to left node
				try {
					dosLeft.writeInt(tempOut[i]);
				} catch (IOException ioe) {
					System.out.println("error in sending left, row=" + i);
					ioe.printStackTrace();
				}
			}
		} 
		System.out.println("Shifted A matrix"); 
		MatrixMultiple.displayMatrix(a);
		// shift b up omitted ...
	}

	
		
	void readA()
	{		for (int i = 0; i < dim; i++) {
			for (int j = 0; j <width; j++) {
				try {
					a[i][j] = disCoor.readInt();
				} catch (IOException ioe) {
					System.out.println("error: " + i + ", " + j);
					ioe.printStackTrace();
				}
			}

		//MatrixMultiple.displayMatrix(a);
	}
	System.out.println("Received  A matrix from Corrdinator");
	MatrixMultiple.displayMatrix(a);
	}
	void readB()
	{		for (int i = 0; i < dim; i++) {
			for (int j = 0; j <width; j++) {
				try {
					b[i][j] = disCoor.readInt();
				} catch (IOException ioe) {
					System.out.println("error: " + i + ", " + j);
					ioe.printStackTrace();
				}
			}
		}
		//MatrixMultiple.displayMatrix(b);
	System.out.println("Received  B matrix from Corrdinator");
	MatrixMultiple.displayMatrix(b);
	}
		
	// shift matrix a toward top 
	void topShift(){	
		
        
		
		for(int i=0; i<dim;i++)
		{
			try
			{
				dosUp.writeInt(b[0][i]);
				//System.out.println("Sending upper block of Worker 0");
			}
			catch(Exception e){
				e.printStackTrace();
			}			
		}
		
		for(int i=1;i<dim;i++)
		{
			for(int j=0;j<dim;j++)
			{
				b[i-1][j]=b[i][j];
				//System.out.println("Local shift done");
			}
		}
		
		for(int i=0;i<dim;i++)
		{
			try{
				b[dim-1][i] = disDown.readInt();
				//System.out.println("reading or not");
			}
			catch(Exception e){
				e.printStackTrace();
			}
		}
		
		System.out.println("Shifted B matrix"); 
		MatrixMultiple.displayMatrix(b);		
	}
	
	
	public static void main(String[] args) {
		if (args.length != 4) {
			System.out.println("usage: java Worker workerID worker-port-num coordinator-ip coordinator-port-num"); 
		} 
		int workerID = Integer.parseInt(args[0]); 
		int portNum = Integer.parseInt(args[1]);
		Worker worker = new Worker(workerID, portNum);
		worker.configurate(args[2], Integer.parseInt(args[3]));

		worker.readA();
		worker.readB();
		worker.calculate();
		
		
		
		/*worker.compute();
		worker.topShift();*/
		try {Thread.sleep(12000);} catch (Exception e) {e.printStackTrace();}
		System.out.println("Done.");
	}
}
