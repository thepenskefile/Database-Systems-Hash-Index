import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.math.BigInteger;
import java.util.Arrays;
import java.nio.ByteBuffer;


public class hashload {

    final static int RECORD_SIZE = 368;
    final static int NUMBER_OF_INDEX_SLOTS = 315000;
    final static int EMPTY_SLOT_INDICATOR = -1;
    final static int INT_BYTE_SIZE = 4;
    
    private static int numberOfCollisions = 0;
    
	// Trim any null bytes from the strings due to the fixed length records
    private static String trimNulls(String str) {
        int pos = str.indexOf(0);
        return pos == -1 ? str : str.substring(0, pos);
    }
    
    private static void readHeapFile(int pageSize) throws IOException {
		RandomAccessFile heapFile = new RandomAccessFile("heap." + pageSize, "r");
		heapFile.seek(0);
		
	     File hashFile = new File("hash." + pageSize);
         RandomAccessFile heapIndex = new RandomAccessFile(hashFile, "rw");

		// Initialise index with -1 to indicate an empty position in all slots
         for(int i = 0; i < NUMBER_OF_INDEX_SLOTS; i++) {
             heapIndex.writeInt(EMPTY_SLOT_INDICATOR);
         }                   
		
    	int yearByteSize = INT_BYTE_SIZE;
    	int blockIdByteSize = INT_BYTE_SIZE;
    	int propertyIdByteSize = INT_BYTE_SIZE;
    	int basePropertyIdByteSize = INT_BYTE_SIZE;
    	int buildingNameByteSize = 65;
    	int streetAddressByteSize = 35;
    	int smallAreaByteSize = 30;
    	int numberFloorsByteSize = INT_BYTE_SIZE;
    	int predominantSpaceUseByteSize = 40;
    	int accessibilityTypeByteSize = 35;
    	int accessibilityTypeDescriptionByteSize = 85;
    	int accessibilityRatingByteSize = INT_BYTE_SIZE;
    	int bicycleSpacesByteSize = INT_BYTE_SIZE;
    	int hasShowersByteSize = INT_BYTE_SIZE;
    	int coordinateByteSize = INT_BYTE_SIZE;
    	int locationByteSize = 30;    	
    	
   	 
	   	 for(int pagePointer = 0; pagePointer < heapFile.length(); pagePointer += pageSize) {
	   		 
				heapFile.seek(pagePointer);
				byte[] pageBytes = new byte[pageSize];
				heapFile.read(pageBytes);
				

				
				for(int recordPointer = 0; recordPointer < pageBytes.length; recordPointer += RECORD_SIZE ) {
					
        			byte[] recordData = Arrays.copyOfRange(pageBytes, recordPointer, recordPointer + RECORD_SIZE);
					int valuePointer = 0;
					
			        int censusYear = new BigInteger(Arrays.copyOfRange(recordData, valuePointer, valuePointer += yearByteSize)).intValue();
			        int blockId = new BigInteger(Arrays.copyOfRange(recordData, valuePointer, valuePointer += blockIdByteSize)).intValue();
			        int propertyId = new BigInteger(Arrays.copyOfRange(recordData, valuePointer, valuePointer += propertyIdByteSize)).intValue();
			        int basePropertyId = new BigInteger(Arrays.copyOfRange(recordData, valuePointer, valuePointer += basePropertyIdByteSize)).intValue();
//			        System.out.println("VALUE POINTER: " + valuePointer);
			        String buildingName = trimNulls(new String(Arrays.copyOfRange(recordData, valuePointer, valuePointer += buildingNameByteSize)));
			        String streetAddress = trimNulls(new String(Arrays.copyOfRange(recordData, valuePointer, valuePointer += streetAddressByteSize)));
			        String smallArea = trimNulls(new String(Arrays.copyOfRange(recordData, valuePointer, valuePointer += smallAreaByteSize)));
			        int constructionYear = new BigInteger(Arrays.copyOfRange(recordData, valuePointer, valuePointer += yearByteSize)).intValue();
			        int refurbishedYear = new BigInteger(Arrays.copyOfRange(recordData, valuePointer, valuePointer += yearByteSize)).intValue();
			        int numberFloors = new BigInteger(Arrays.copyOfRange(recordData, valuePointer, valuePointer += numberFloorsByteSize)).intValue();
			        String predominantSpaceUse = trimNulls(new String(Arrays.copyOfRange(recordData, valuePointer, valuePointer += predominantSpaceUseByteSize)));
			        String accessibilityType = trimNulls(new String(Arrays.copyOfRange(recordData, valuePointer, valuePointer += accessibilityTypeByteSize)));
			        String accessibilityTypeDescription = trimNulls(new String(Arrays.copyOfRange(recordData, valuePointer, valuePointer += accessibilityTypeDescriptionByteSize)));
			        int accessibilityRating = new BigInteger(Arrays.copyOfRange(recordData, valuePointer, valuePointer += accessibilityRatingByteSize)).intValue();
			        int bicycleSpaces = new BigInteger(Arrays.copyOfRange(recordData, valuePointer, valuePointer += bicycleSpacesByteSize)).intValue();
			        int hasShowers = new BigInteger(Arrays.copyOfRange(recordData, valuePointer, valuePointer += hasShowersByteSize)).intValue();
			        float xCoordinate = ByteBuffer.wrap(Arrays.copyOfRange(recordData, valuePointer, valuePointer += coordinateByteSize)).getFloat();
			        float yCoordinate = ByteBuffer.wrap(Arrays.copyOfRange(recordData, valuePointer, valuePointer += coordinateByteSize)).getFloat();
			        String location = trimNulls(new String(Arrays.copyOfRange(recordData, valuePointer, valuePointer += locationByteSize)));
			        
			        byte[] buildingNameBytes = Arrays.copyOfRange(recordData, 16, 16 + buildingNameByteSize);
//			        String buildingName = trimNulls(new String(Arrays.copyOfRange(recordData, valuePointer, valuePointer += buildingNameByteSize)));
//			        System.out.println("BUILDING NAME: " + trimNulls(new String(buildingNameBytes)));
			        
					int hashIndex = Math.abs((Arrays.hashCode(buildingNameBytes)) % NUMBER_OF_INDEX_SLOTS);
					
					writeToIndex(heapIndex, hashIndex, pagePointer + recordPointer);

			        
			        if(buildingName.equals("RHUMBARALLAS")) {
			        	System.out.println("HASH INDEX: " + hashIndex);
			        	System.out.println("RECORD POINTER: " + recordPointer);

				   	 	System.out.println("Census year: " + censusYear);
					   	System.out.println("Block ID: " + blockId);
					   	System.out.println("Property ID: " + propertyId);
					   	System.out.println("Base property ID: " + basePropertyId);
					   	System.out.println("Building name: " + buildingName);
					   	System.out.println("Street address: " + streetAddress);
					   	System.out.println("Small area: " + smallArea);
					   	System.out.println("Construction year: " + constructionYear);
					   	System.out.println("Refurbished year: " + refurbishedYear);
					   	System.out.println("Number floors: " + numberFloors);
					   	System.out.println("Predominant space use: " + predominantSpaceUse);
					   	System.out.println("Accessibility type: " + accessibilityType);
					   	System.out.println("AccessibilityTypeDescription: " + accessibilityTypeDescription);
					   	System.out.println("AccessibilityRating: " + accessibilityRating);
					   	System.out.println("Bicycle spaces: " + bicycleSpaces);
					   	System.out.println("Has showers: " + hasShowers);
					   	System.out.println("x coordinate: " + xCoordinate);
					   	System.out.println("y coordinate: " + yCoordinate);
					   	System.out.println("Location: " + location);
					   	System.out.println("=====================================");
			        }					
					
				}	   		 
	   	 }
	   	 
	   	System.out.println("NUMBER OF COLLISIONS: " + numberOfCollisions);
	   	heapFile.close();
	   	heapIndex.close();
	   	 
    }
    
    private static void writeToIndex(RandomAccessFile heapIndex, int hashIndex, int recordPointer) {
    	
        try {
        	
        	int currentHashIndex = hashIndex;
        	
        	while(true) {        		
                heapIndex.seek(currentHashIndex);
                int slotHash = heapIndex.readInt();
    			heapIndex.seek(currentHashIndex);
    			
    			if(slotHash == EMPTY_SLOT_INDICATOR) {
    				// There has not been a collision
    	             heapIndex.writeInt(recordPointer);
    	             break;
    			}
    			else {
    				// There has been a collision
    				numberOfCollisions++;
    				
    				// Slot is occupied, so check the next one
    				currentHashIndex += INT_BYTE_SIZE;
    				
    				// If the end of the file is reached, go back to the start
    				if(currentHashIndex >= NUMBER_OF_INDEX_SLOTS -1) {
    					currentHashIndex = 0;
    				}
    			}
        	}			
			
		} catch (IOException e) {
			System.out.println("Error trying to write to hash index file");
		}
    }

	
	public static void main(String[] args) {
        int pageSize = 0;

        long startTime = System.nanoTime();

        if (args.length != 1) {
            System.err.println("Error! Usage: java hashload pagesize");
        } else {
            try {
                pageSize = Integer.parseInt(args[0]);
            } catch(NumberFormatException e) {
                System.err.println("Error! pagesize value must be an integer.");
            }
        }
        
        try {
			readHeapFile(pageSize);
		} catch (FileNotFoundException e) {
			System.out.println("Could not find file " + pageSize + ".heap");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
	}
	
}
