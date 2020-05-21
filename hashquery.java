import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class hashquery {
    
    final static int RECORD_SIZE = 368;
    final static int INT_BYTE_SIZE = 4;
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
    
    final static int NUMBER_OF_INDEX_SLOTS = 315000;
    final static int NUMBER_OF_BUCKETS = 63000;
    final static int BUCKET_SIZE = 5 * INT_BYTE_SIZE;
    final static int PAGE_NUMBER_OFFSET = 3;
    final static int EMPTY_SLOT_INDICATOR = -1;
    final static int START_POINTER_POSITION = 0;
    
    //////////////////////////////////////////
    
    /**
     * Function to trim any null bytes from strings that were used to pad the string for the fixed length record
     * 
     */
    private static String trimNulls(String str) {
        int position = str.indexOf(0);
        return position == -1 ? str : str.substring(0, position);
    }
	
	private static void searchIndex(int pageSize, String searchText, RandomAccessFile heapFile, RandomAccessFile hashFile) throws IOException {
                         
		// Convert search text to bytes
		byte[] buildingNameBytes = Arrays.copyOf(searchText.getBytes(), BUILDING_NAME_BYTE_SIZE);
		
		// Hash the search text
		int hashIndex = Math.abs((Arrays.hashCode(buildingNameBytes)) % NUMBER_OF_BUCKETS) * BUCKET_SIZE;		

         try {
        	 
          	int currentBucketNumber = hashIndex;
                   		         		
     		hashFile.seek(currentBucketNumber);
     		
     		int recordOffset = hashFile.readInt();
            
            for(int i = recordOffset; i < recordOffset + (RECORD_SIZE * (BUCKET_SIZE / INT_BYTE_SIZE)); i += RECORD_SIZE) {
            	
            	heapFile.seek(i);
				byte[] recordData = new byte[RECORD_SIZE];
				heapFile.read(recordData);
				
				int valuePointer = 0;
				
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
		        
		        if(buildingName.equals(searchText)) {
					System.out.println("Census year: " + censusYear + "\n" + "Block ID: " + blockId + "\n" + "Property ID: " + propertyId + "\n" + "Base property ID: " + basePropertyId + "\n" + 
										"Building name: " + buildingName + "\n" + "Street address: " + streetAddress + "\n" + "Small area: " + smallArea + "\n" + "Construction year: " + constructionYear + "\n" + 
										"Refurbished year: " + refurbishedYear + "\n" + "Number floors: " + numberFloors + "\n" + "Predominant space use: " + predominantSpaceUse + "\n" + 
										"Accessibility type: " + accessibilityType + "\n" + "Accessibility type description: " + accessibilityTypeDescription + "\n" + 
										"Accessibility rating: " + accessibilityRating + "\n" + "Bicycle spaces: " + bicycleSpaces + "\n" + "Has showers: " + hasShowers + "\n" + 
										"x coordinate: " + xCoordinate + "\n" + "y coordinate: " + yCoordinate + "\n" + "Location: " + location);
					
					System.out.println("=====================================");
					
		        }

            	
            }
 			
 		} catch (IOException e) {
 			System.out.println("Record not found");
 		}
         
	}
	
	
	public static void main(String[] args) {
				
        int pageSize = 0;
        String queryText = "";
                
        if (args.length < 2) {
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
        
        long startTime = System.nanoTime();

        try {
        	
        	File heapFile = new File("heap." + pageSize);
    		RandomAccessFile heap = new RandomAccessFile(heapFile, "r");
   	     	File hashFile = new File("hash." + pageSize);
            RandomAccessFile hash = new RandomAccessFile(hashFile, "r");
            System.out.println("QUERY: !" + queryText + "!");
			searchIndex(pageSize, queryText, heap, hash);
	         
	        heap.close();
	        hash.close();
	        
		} catch (FileNotFoundException e) {
			System.out.println("Could not find file " + pageSize + ".heap");
		} catch (IOException e) {
			e.printStackTrace();
		}
        
        long endTime = System.nanoTime();
		
	    double totalTime = (double)(endTime - startTime) / 1_000_000_000;
	    
	    System.out.println("Data records found in: " + totalTime + " seconds");
		
	}

}
