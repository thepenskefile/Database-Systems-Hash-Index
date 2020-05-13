import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;

public class hashload {

    final static int RECORD_SIZE = 368;
    final static int NUMBER_OF_INDEX_SLOTS = 315000;
    final static int EMPTY_SLOT_INDICATOR = -1;
    final static int INT_BYTE_SIZE = 4;
    final static int START_POINTER_POSITION = 0;
    final static int NUMBER_OF_ARGS = 1;
    
    //////////////////////////////////////////
    
    final static int YEAR_BYTE_SIZE = INT_BYTE_SIZE;
    final static int BLOCK_ID_BYTE_SIZE = INT_BYTE_SIZE;
    final static int PROPERTY_ID_BYTE_SIZE = INT_BYTE_SIZE;
    final static int BASE_PROPERTY_ID_BYTE_SIZE = INT_BYTE_SIZE;
    final static int BUILDING_NAME_BYTE_SIZE = 65;
    final static int STREET_ADDRESS_BYTE_SIZE = 35;
    final static int SMALL_AREA_BYTE_SIZE = 30;
    final static int NUMBER_FLOORS_BYTE_SIZE = INT_BYTE_SIZE;
    final static int PREDOMINANT_SPACE_USE_BYTE_SIZE = 40;
    final static int ACCESSIBILITY_TYPE_BYTE_SIZE = 35;
    final static int ACCESSIBILITY_TYPE_DESCRIPTION_BYTE_SIZE = 85;
    final static int ACCESSIBILITY_RATING_BYTE_SIZE = INT_BYTE_SIZE;
    final static int BICYCLE_SPACES_BYTE_SIZE = INT_BYTE_SIZE;
    final static int HAS_SHOWERS_BYTE_SIZE = INT_BYTE_SIZE;
    final static int COORDINATES_BYTE_SIZE = INT_BYTE_SIZE;
    final static int LOCATION_BYTE_SIZE = 30;
    final static int BUILDING_NAME_OFFSET = YEAR_BYTE_SIZE + BLOCK_ID_BYTE_SIZE + PROPERTY_ID_BYTE_SIZE + BASE_PROPERTY_ID_BYTE_SIZE;    	
    		
    //////////////////////////////////////////
    
    private static int numberOfCollisions = 0;
    private static int numberOfRecordsIndexed = 0;
    
    //////////////////////////////////////////
    
    private static void readHeapFile(int pageSize, RandomAccessFile heapFile, RandomAccessFile hashFile) throws IOException  {
   	
    	initialiseIndexFile(hashFile);
    	
    	heapFile.seek(START_POINTER_POSITION);

    	// Search through pages
	   	for(int pagePointer = START_POINTER_POSITION; pagePointer < heapFile.length(); pagePointer += pageSize) {
	   		 
			heapFile.seek(pagePointer);
			byte[] pageBytes = new byte[pageSize];
			heapFile.read(pageBytes);	
			
			// Search through records
			for(int recordPointer = START_POINTER_POSITION; recordPointer < pageBytes.length; recordPointer += RECORD_SIZE ) {
				
				byte[] recordData = Arrays.copyOfRange(pageBytes, recordPointer, recordPointer + RECORD_SIZE);			        
		        byte[] buildingNameBytes = Arrays.copyOfRange(recordData, BUILDING_NAME_OFFSET, BUILDING_NAME_OFFSET + BUILDING_NAME_BYTE_SIZE);			        
				int hashIndex = Math.abs((Arrays.hashCode(buildingNameBytes)) % NUMBER_OF_INDEX_SLOTS);					
				writeToIndex(hashFile, hashIndex, pagePointer + recordPointer);
				
			}	   		 
	   	}
	   	
    }
    
    private static void writeToIndex(RandomAccessFile heapIndex, int hashIndex, int recordPointer) {
    	
        try {
        	
        	int currentHashIndex = hashIndex;
        	boolean recordSuccessfullyIndexed = false;
        	
        	while(!recordSuccessfullyIndexed) {
        		
                heapIndex.seek(currentHashIndex);
                int slotPointer = heapIndex.readInt();
    			heapIndex.seek(currentHashIndex);    			

    			if(slotPointer == EMPTY_SLOT_INDICATOR) {
        			
    				// There has not been a collision, and we can insert the pointer at this index
    	            heapIndex.writeInt(recordPointer);
    	            numberOfRecordsIndexed++;
    	             
    	            recordSuccessfullyIndexed = true;
    			}
    			
    			else {
    				
        			// There has been a collision, and we need to use linear probing to find the next available slot to insert the pointer  				
    				numberOfCollisions++;
    				
    				// Move to the next slot
    				currentHashIndex += INT_BYTE_SIZE;
    				
    				// If the end of the file is reached, go back to the beginning and continue searching for an available slot
    				if(currentHashIndex >= NUMBER_OF_INDEX_SLOTS -1) {
    					currentHashIndex = START_POINTER_POSITION;
    				}
    			}
        	}			
			
		} catch (IOException e) {
			System.out.println("Error trying to write to hash index file");
		}
    }
    
	/**
	 * Function to initialise all index slots with -1 to indicate the slot it empty
	 */  
    private static void initialiseIndexFile(RandomAccessFile hashFile) {
  	
    	for(int i = START_POINTER_POSITION; i < NUMBER_OF_INDEX_SLOTS; i++) {
    		
        	try {
				hashFile.writeInt(EMPTY_SLOT_INDICATOR);
			} catch (IOException e) {				
                System.err.println("Error! Could not initialise index file");
			}
        }
    }

	
	public static void main(String[] args) {
		
        int pageSize = 0;

        long startTime = System.nanoTime();

        if (args.length != NUMBER_OF_ARGS) {
        	
            System.err.println("Error! Usage: java hashload pagesize");
            
        } else {
        	
        	try {            	
            	pageSize = Integer.parseInt(args[0]);                
            } catch(NumberFormatException e) {            	
                System.err.println("Error! pagesize value must be an integer.");
            }
        }
        
        try {
        	
        	File heapFile = new File("heap." + pageSize);
    		RandomAccessFile heap = new RandomAccessFile(heapFile, "r");
   	     	File hashFile = new File("hash." + pageSize);
            RandomAccessFile hash = new RandomAccessFile(hashFile, "rw");

			readHeapFile(pageSize, heap, hash);
			
			heap.close();
			hash.close();		
			
		} catch (FileNotFoundException e) {			
			System.out.println("Error finding file");			
		} catch (IOException e) {
			e.printStackTrace();
		}
        
		long endTime = System.nanoTime(); 
	    double totalTime = (double)(endTime - startTime) / 1_000_000_000;
	    
	    System.out.println("Data indexed in: " + totalTime + " seconds");
	    System.out.println("Number of records indexed: " + numberOfRecordsIndexed);
	    System.out.println("Number of collisions: " + numberOfCollisions);
	}	
}
