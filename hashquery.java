import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public class hashquery {
    
    final static int RECORD_SIZE = 368;
    final static int INT_BYTE_SIZE = 4;
    final static int NUMBER_OF_ARGS = 2;
    
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
    
    final static int NUMBER_OF_BUCKETS = 17737;
    final static int BUCKET_SIZE = 30 * INT_BYTE_SIZE;
    final static int PAGE_NUMBER_OFFSET = 3;
    final static int EMPTY_SLOT_INDICATOR = -1;
    final static int START_POINTER_POSITION = 0;
    final static int START_COUNTER = 0;
    final static char START_RANGE_CHAR = 'J';
    final static char END_RANGE_CHAR = 'S';
    final static String RANGE_ARG = "-r";
    
    //////////////////////////////////////////
    
    /**
     * Function to trim any null bytes from strings that were used to pad the string for the fixed length record
     * 
     */
    private static String trimNulls(String str) {
        int position = str.indexOf(0);
        return position == -1 ? str : str.substring(0, position);
    }
	
    /**
     * Function to search for a record given an input string
     * @param pageSize
     * @param searchText
     * @param heapFile
     * @param hashFile
     * @return the number of records found
     * @throws IOException
     */
	private static int searchIndex(int pageSize, String searchText, RandomAccessFile heapFile, RandomAccessFile hashFile) throws IOException {
                         
      	int numberOfRecordsFound = START_COUNTER;

		// Convert search text to bytes
		byte[] buildingNameBytes = Arrays.copyOf(searchText.getBytes(), BUILDING_NAME_BYTE_SIZE);	

		// Hash the search text
		int hashIndex = Math.abs((Arrays.hashCode(buildingNameBytes)) % NUMBER_OF_BUCKETS) * BUCKET_SIZE;

         try {
        	
        	// If the hashed value points to a bucket with -1, then that means there has been no records inserted to that bucket and we can stop looking
    		if(hashIndex == EMPTY_SLOT_INDICATOR) {
    			return numberOfRecordsFound;
    		}
                 		
     		hashFile.seek(hashIndex);
     		byte[] bucketData = new byte[BUCKET_SIZE];
     		hashFile.read(bucketData); 		
     		
     		// Loop through the entire bucket one index key at a time
     		for(int bucketPointer = START_POINTER_POSITION; bucketPointer < bucketData.length; bucketPointer += INT_BYTE_SIZE) {
     			
		        int recordPointer = new BigInteger(Arrays.copyOfRange(bucketData, bucketPointer, bucketPointer + INT_BYTE_SIZE)).intValue();
		        
		        // If the hashed value points to a slot with -1, then that means the bucket contains no more records and we can stop looking
	    		if(recordPointer == EMPTY_SLOT_INDICATOR) {
	    			return numberOfRecordsFound;
	    		}
	    		
	    		// Find the position in the heapfile of where the record pointer is pointing
		        heapFile.seek(recordPointer);
		        
		        // Read the entire record at that position
		        byte[] recordData = new byte[RECORD_SIZE];
				heapFile.read(recordData);
		        
				// Extract the building name from that record
		        String buildingName = trimNulls(new String(Arrays.copyOfRange(recordData, BUILDING_NAME_OFFSET, BUILDING_NAME_OFFSET +BUILDING_NAME_BYTE_SIZE)));
		        
		        // If the building name of the record matches the search text input, then print the record
		        if(buildingName.equals(searchText)) {
		        	numberOfRecordsFound++;
		        	printRecord(recordData);					
		        }

     		}
 			
 		} catch (IOException e) {
 			System.err.println("Record not found");
 		}
         
        return numberOfRecordsFound;
	}
	
	/**
	 * Function to perform a range query. The range query searches for all records where the first letter of the building name is between "J" and "S"
	 * @param pageSize
	 * @param searchText
	 * @param heapFile
	 * @param hashFile
	 * @return the number of records found
	 * @throws IOException
	 */
	private static int searchRangeQuery(int pageSize, String searchText, RandomAccessFile heapFile, RandomAccessFile hashFile) throws IOException {
		int numberOfRecordsFound = START_COUNTER;
 		hashFile.seek(START_POINTER_POSITION);
 		byte[] indexData = new byte[BUCKET_SIZE * NUMBER_OF_BUCKETS];
 		hashFile.read(indexData);
		
 		// Read through index one key at a time
		for(int indexPointer = START_POINTER_POSITION; indexPointer < indexData.length; indexPointer += INT_BYTE_SIZE) {
			int recordPointer = new BigInteger(Arrays.copyOfRange(indexData, indexPointer, indexPointer + INT_BYTE_SIZE)).intValue();
    		if(recordPointer != EMPTY_SLOT_INDICATOR) {
    			heapFile.seek(recordPointer);
  			
		        // Read the entire record at that position
		        byte[] recordData = new byte[RECORD_SIZE];
				heapFile.read(recordData);
				
				// Extract the building name from that record
		        String buildingName = trimNulls(new String(Arrays.copyOfRange(recordData, BUILDING_NAME_OFFSET, BUILDING_NAME_OFFSET +BUILDING_NAME_BYTE_SIZE)));	
		        
		        // If the first char of the building name in the record is in the correct range
		        if(buildingName.charAt(0) >= START_RANGE_CHAR && buildingName.charAt(0) <= END_RANGE_CHAR) {
		        	numberOfRecordsFound++;
		        	printRecord(recordData);					
		        }
    		}
		}
		
		return numberOfRecordsFound;
	}
	
	/**
	 * Function to print record values
	 * @param recordData the record data in bytes
	 */
	private static void printRecord(byte[] recordData) {
		int valuePointer = START_POINTER_POSITION;
		
        int censusYear = new BigInteger(Arrays.copyOfRange(recordData, valuePointer, valuePointer += YEAR_BYTE_SIZE)).intValue();
        int blockId = new BigInteger(Arrays.copyOfRange(recordData, valuePointer, valuePointer += BLOCK_ID_BYTE_SIZE)).intValue();
        int propertyId = new BigInteger(Arrays.copyOfRange(recordData, valuePointer, valuePointer += PROPERTY_ID_BYTE_SIZE)).intValue();
        int basePropertyId = new BigInteger(Arrays.copyOfRange(recordData, valuePointer, valuePointer += BASE_PROPERTY_ID_BYTE_SIZE)).intValue();
        String buildingName = trimNulls(new String(Arrays.copyOfRange(recordData, valuePointer, valuePointer += BUILDING_NAME_BYTE_SIZE)));
        String streetAddress = trimNulls(new String(Arrays.copyOfRange(recordData, valuePointer, valuePointer += STREET_ADDRESS_BYTE_SIZE)));
        String smallArea = trimNulls(new String(Arrays.copyOfRange(recordData, valuePointer, valuePointer += SMALL_AREA_BYTE_SIZE)));
        int constructionYear = new BigInteger(Arrays.copyOfRange(recordData, valuePointer, valuePointer += YEAR_BYTE_SIZE)).intValue();
        int refurbishedYear = new BigInteger(Arrays.copyOfRange(recordData, valuePointer, valuePointer += YEAR_BYTE_SIZE)).intValue();
        int numberFloors = new BigInteger(Arrays.copyOfRange(recordData, valuePointer, valuePointer += NUMBER_FLOORS_BYTE_SIZE)).intValue();
        String predominantSpaceUse = trimNulls(new String(Arrays.copyOfRange(recordData, valuePointer, valuePointer += PREDOMINANT_SPACE_USE_BYTE_SIZE)));
        String accessibilityType = trimNulls(new String(Arrays.copyOfRange(recordData, valuePointer, valuePointer += ACCESSIBILITY_TYPE_BYTE_SIZE)));
        String accessibilityTypeDescription = trimNulls(new String(Arrays.copyOfRange(recordData, valuePointer, valuePointer += ACCESSIBILITY_TYPE_DESCRIPTION_BYTE_SIZE)));
        int accessibilityRating = new BigInteger(Arrays.copyOfRange(recordData, valuePointer, valuePointer += ACCESSIBILITY_RATING_BYTE_SIZE)).intValue();
        int bicycleSpaces = new BigInteger(Arrays.copyOfRange(recordData, valuePointer, valuePointer += BICYCLE_SPACES_BYTE_SIZE)).intValue();
        int hasShowers = new BigInteger(Arrays.copyOfRange(recordData, valuePointer, valuePointer += HAS_SHOWERS_BYTE_SIZE)).intValue();
        float xCoordinate = ByteBuffer.wrap(Arrays.copyOfRange(recordData, valuePointer, valuePointer += COORDINATES_BYTE_SIZE)).getFloat();
        float yCoordinate = ByteBuffer.wrap(Arrays.copyOfRange(recordData, valuePointer, valuePointer += COORDINATES_BYTE_SIZE)).getFloat();
        String location = trimNulls(new String(Arrays.copyOfRange(recordData, valuePointer, valuePointer += LOCATION_BYTE_SIZE)));
        
		System.out.println("Census year: " + censusYear + "\n" + "Block ID: " + blockId + "\n" + "Property ID: " + propertyId + "\n" + "Base property ID: " + basePropertyId + "\n" + 
				"Building name: " + buildingName + "\n" + "Street address: " + streetAddress + "\n" + "Small area: " + smallArea + "\n" + "Construction year: " + constructionYear + "\n" + 
				"Refurbished year: " + refurbishedYear + "\n" + "Number floors: " + numberFloors + "\n" + "Predominant space use: " + predominantSpaceUse + "\n" + 
				"Accessibility type: " + accessibilityType + "\n" + "Accessibility type description: " + accessibilityTypeDescription + "\n" + 
				"Accessibility rating: " + accessibilityRating + "\n" + "Bicycle spaces: " + bicycleSpaces + "\n" + "Has showers: " + hasShowers + "\n" + 
				"x coordinate: " + xCoordinate + "\n" + "y coordinate: " + yCoordinate + "\n" + "Location: " + location);
		System.out.println("=====================================");
	}
	
	public static void main(String[] args) {
				
        int pageSize = 0;
        String queryText = "";
        if (args.length < NUMBER_OF_ARGS) {
            System.err.println("Error! Usage: java hashquery querytext pagesize");
        } else {
            try {
            	String[] searchTextArray = Arrays.copyOf(args, args.length - 1);
        		queryText = String.join(" ", searchTextArray);
            } catch(Exception e) {
                System.err.println("Error parsing the querytext");
            }
            
            try {
                pageSize = Integer.parseInt(args[args.length - 1]);
            } catch(NumberFormatException e) {
                System.err.println("Error! pagesize value must be an integer.");
            }
        }
        
        int numberOfRecordsFound = 0;
        long startTime = System.nanoTime();

        try {
       	
        	File heapFile = new File("heap." + pageSize);
    		RandomAccessFile heap = new RandomAccessFile(heapFile, "r");
   	     	File hashFile = new File("hash." + pageSize);
            RandomAccessFile hash = new RandomAccessFile(hashFile, "r");
            if(queryText.equals(RANGE_ARG)) {
            	numberOfRecordsFound = searchRangeQuery(pageSize, queryText, heap, hash);
            }
            else {
            	numberOfRecordsFound = searchIndex(pageSize, queryText, heap, hash);
            }
				         
	        heap.close();
	        hash.close();
	        
		} catch (FileNotFoundException e) {
			System.err.println("Could not find file " + pageSize + ".heap");
		} catch (IOException e) {
			System.err.println("There was an error attempting to read " + pageSize + ".heap");
		}       

	    long totalTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
        System.out.println("Number of records found: " + numberOfRecordsFound);
	    System.out.println("Data records found in: " + totalTime + " ms");
		
	}

}
