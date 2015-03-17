package hw2;



import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

public class Indexer {

	/*
	 * This class will create my own index from the given location that will
	 * replace the ElasticSearch calls.
	 * This class will be able to handle large number of documents and terms
	 * without using excessive memory or disk I/O.
	 **/
	private static int termIndex = 1;
	private static int lineBeginning = 0;
	private static int documentID = 1;
	private static Map<String, String> termIdMap = new LinkedHashMap<String, String>();
	private static Map<String, Integer> termTTF = new LinkedHashMap<String, Integer>();
	private static Map<Integer,String> docIdMap = new LinkedHashMap<Integer,String>();
	private static Map<String, String> stopWords = new LinkedHashMap<String, String>();
	private static Map<Integer,Integer> docLenMap = new LinkedHashMap<Integer,Integer>();
	private static String localUrl = "C:/College/temp/HW2-IR/src/LocalMemory/";
	private static String otherFilesUrl = "C:/College/temp/HW2-IR/src/OtherFiles/";
	private static String invertedIndexFileLocation = "C:/College/temp/HW2-IR/src/InvertedIndex/";
	private static String stopWordsFileLocation = "C:/College/IR/elasticsearch-1.4.2/config/stoplist.txt";

	/**
	 * The main function will handle most of the file operations. It will read the files in the 
	 * given location and then it will send it to the parser which will index only 1000 documents 
	 * at any given time
	 */
	
	public static void main(String[] args) {
		Indexer indexer = new Indexer();
		try {
			URL url = Indexer.class.getResource("/resources/AP_DATA/ap89_collection/"); 
			//URL url = Indexer.class.getResource("/resources/AP_DATA/test/"); 
			File stopWordsFile = new File(stopWordsFileLocation);
			BufferedReader stopWordsReader = new BufferedReader(new FileReader(stopWordsFile));
			String stopWordLine;
			while((stopWordLine = stopWordsReader.readLine())!= null){
				stopWords.put(stopWordLine, "");
			}
			if (url == null) {
				throw new IOException("ERROR - missing resourse data folder");
			} else {
				File folder;
				folder = new File(url.toURI());
				int fileNum = 1;
				File[] listOfFiles = folder.listFiles();
				for (int i = 0; i < listOfFiles.length; i++) {
					File file = listOfFiles[i];
					if(file.isFile())
					{
						System.out.println("file number : "+fileNum);
						indexer.createTupleFile(file);
						fileNum++;
					}
				}
			}


			File termIdMapFile = new File(otherFilesUrl+"termIdMap.txt");
			File termTTFFile = new File(otherFilesUrl+"termTTF.txt");
			File docIdMapFile = new File(otherFilesUrl+"docIdMap.txt");
			File docLenMapFile = new File(otherFilesUrl+"docLenMap.txt");
			termIdMapFile.createNewFile();
			termTTFFile.createNewFile();
			docIdMapFile.createNewFile();
			docLenMapFile.createNewFile();

			FileWriter termIdMapFileWriter = new FileWriter(termIdMapFile.getAbsoluteFile());
			BufferedWriter termIdMapFileBW = new BufferedWriter(termIdMapFileWriter);

			FileWriter termTTFFileWriter = new FileWriter(termTTFFile.getAbsoluteFile()); // serialize the hashmap for TTF
			BufferedWriter termTTFFileBW = new BufferedWriter(termTTFFileWriter);

			FileWriter docIdMapFileWriter = new FileWriter(docIdMapFile.getAbsoluteFile());
			BufferedWriter docIdMapFileBW = new BufferedWriter(docIdMapFileWriter);
			
			FileWriter docLenMapFileWriter = new FileWriter(docLenMapFile.getAbsoluteFile());
			BufferedWriter docLenMapFileBW = new BufferedWriter(docLenMapFileWriter);

			for(Map.Entry<String, String> termIdMapContent : termIdMap.entrySet()){
				termIdMapFileBW.append(termIdMapContent.getKey()+" "+termIdMapContent.getValue()+"\n");
			}

			for(Map.Entry<String, Integer> termTTFContent : termTTF.entrySet()){
				termTTFFileBW.append(termTTFContent.getKey()+" "+termTTFContent.getValue()+"\n");
			}

			for(Map.Entry<Integer, String> docIdMapContent: docIdMap.entrySet()){
				docIdMapFileBW.append(docIdMapContent.getKey() + " "+ docIdMapContent.getValue()+"\n");
			}
			
			for(Map.Entry<Integer, Integer> docLenMapContent: docLenMap.entrySet()){
				docLenMapFileBW.append(docLenMapContent.getKey() + " "+ docLenMapContent.getValue()+"\n");
			}

			termIdMapFileBW.close();
			termTTFFileBW.close();
			docIdMapFileBW.close();
			docLenMapFileBW.close();
			termIdMapFileWriter.close();
			termTTFFileWriter.close();
			docIdMapFileWriter.close();
			docLenMapFileWriter.close();

			// serialize LinkedHashMap
			FileOutputStream fos = new FileOutputStream("TermIdLinkedHashMap.ser");
			ObjectOutputStream oos = new ObjectOutputStream(fos);
			oos.writeObject(termIdMap);
			oos.close();
			fos.close();

			FileOutputStream fos2 = new FileOutputStream("DocIdLinkedHashMap.ser");
			ObjectOutputStream oos2 = new ObjectOutputStream(fos2);
			oos2.writeObject(docIdMap);
			oos2.close();
			fos2.close();

			indexer.setupInvertedIndex();
		}catch (URISyntaxException e) {
			e.printStackTrace();
		}catch (IOException e) {
			e.printStackTrace();
		}
	}

	
	/**
	 * Sets the inverted index for every 1000 documents so that 
	 * large memory is not utilized and thus making this program
	 * system memory independent
	 */ 
	public void setupInvertedIndex(){
		try {
			// de-serialize LinkedHashMaps
			//******************************************************
			FileInputStream fis = new FileInputStream("TermIdLinkedHashMap.ser");
			ObjectInputStream ois = new ObjectInputStream(fis);
			Map<String, String> localTermIdMap = (LinkedHashMap) ois.readObject();
			ois.close();
			fis.close();

			FileInputStream fis2 = new FileInputStream("DocIdLinkedHashMap.ser");
			ObjectInputStream ois2 = new ObjectInputStream(fis2);
			Map<Integer, String> localDocIdMap = (LinkedHashMap) ois2.readObject();
			ois.close();
			fis.close();
			//******************************************************
			Map<String, ArrayList<Integer>> termOffsetMap = new LinkedHashMap<String, ArrayList<Integer>>();
			int flag = 0;
			int someCounter = 0;
			for(Map.Entry<String, String> termIdMapContent : localTermIdMap.entrySet()){
				termOffsetMap.put(termIdMapContent.getValue(), new ArrayList<Integer>());  // has (termID : Offset list)
			}

			File tokenDirectory = new File(localUrl);          // Each line is a document. read each line in SB until 1000 is reached
			File[] files = tokenDirectory.listFiles();
			StringBuilder documents = new StringBuilder();
			for(int i = 0; i< files.length; i++){
				File file = files[i];
				if(file.isFile()){
					BufferedReader br = new BufferedReader(new FileReader(file));
					String currentLine;
					while((currentLine = br.readLine())!= null){
						flag++;
						if(flag <= 1000){
							documents.append(currentLine+"\n");
						}
						if(flag == 1000){
							System.out.println("Creating inverted index for 1000 documents set : "+(someCounter++));
							termOffsetMap = createInvertedIndex(termOffsetMap, documents);
							documents.setLength(0);
							flag = 0;
						}

					}
				}
			}
			if(flag > 0){
				termOffsetMap = createInvertedIndex(termOffsetMap, documents);
				documents.setLength(0);
				flag = 0;
			}

			createCatalogAndFullIndex(termOffsetMap);

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}catch(IOException e){
			e.printStackTrace();
		}catch(ClassNotFoundException e){
			e.printStackTrace();
		}
	}

	/**
	 * Creates a catalog file that used for mapping terms and offset positions 
	 * in the inverted index file
	 * @param termOffsetMap  a map between term and its corresponding offset positions 
	 *                       in the inverted index file
	 */
	private void createCatalogAndFullIndex(Map<String, ArrayList<Integer>> termOffsetMap) {
		try{
			System.out.println("Creating the catalog and final index files");
			long indexLineOffset = 0;

			RandomAccessFile raf = new RandomAccessFile(invertedIndexFileLocation+"invertedIndex.txt", "r");

			File finalIndexFile = new File(invertedIndexFileLocation+"FinalInvertedIndex.txt");
			if(!finalIndexFile.exists()){
				finalIndexFile.createNewFile();
			}
			FileWriter fw = new FileWriter(finalIndexFile.getAbsoluteFile(), true);
			BufferedWriter addToIndex = new BufferedWriter(fw) ;

			File catalogFile = new File(invertedIndexFileLocation+"FinalCatalog.txt");
			if(!catalogFile.exists()){
				catalogFile.createNewFile();
			}
			FileWriter fw2 = new FileWriter(catalogFile.getAbsoluteFile(), true);
			BufferedWriter addToCatalog = new BufferedWriter(fw2) ;

			StringBuilder record = new StringBuilder();
			for(Map.Entry<String, ArrayList<Integer>> termOffset : termOffsetMap.entrySet()){
				//				record.append(termOffset.getKey()+":");
				for(int offset : termOffset.getValue()){
					raf.seek(offset);
					String line = raf.readLine();
					if(line.length() > 1)
						record.append(line.split(":")[1]);
				}
				addToIndex.append(record.append("\n"));
				addToCatalog.append(termOffset.getKey()+" : "+indexLineOffset+"\n");
				indexLineOffset += record.length();
				record.setLength(0);
			}
			addToIndex.close();
			addToCatalog.close();
			fw.close();
			fw2.close();

		}catch(IOException e){
			e.printStackTrace();
		}
	}

	/**
	 * Creates a partial inverted index that contains terms and DBlocks and returns the 
	 * offset positions after creating the partial index
	 * @param termOffsetMap  a map between term and its corresponding offset positions 
	 *                       in the inverted index file
	 * @param documentsList  contains the contents from the documents read
	 * @return               term and offset map for a partial inverted index
	 */
	// tuple = (termid, docid, position). termDocBlock = <termid, <docId, dblock>>
	private Map<String, ArrayList<Integer>> createInvertedIndex(Map<String, ArrayList<Integer>> termOffsetMap, StringBuilder documentsList) {
		LinkedHashMap<String, LinkedHashMap<String, DBlock>> termDocBlock = new LinkedHashMap<String, LinkedHashMap<String, DBlock>>();
		DBlock tempDBlock;
		String[] documents = documentsList.toString().split("\n");
		for(int i = 0; i< documents.length; i++){
			//			System.out.println("doc : "+i);
			String[] tuples = documents[i].split("#");
			for(int j = 0; j<tuples.length; j++){
				String[] tuple = tuples[j].split(" ");
				if(tuple.length > 1){

					if(termDocBlock.containsKey(tuple[0])){                              // contains term
						if(termDocBlock.get(tuple[0]).containsKey(tuple[1])){            // contains docid
							tempDBlock = termDocBlock.get(tuple[0]).get(tuple[1]);
							tempDBlock.update(tuple[2]);
							termDocBlock.get(tuple[0]).put(tuple[1], tempDBlock);
						}else{
							DBlock dBlock = new DBlock(tuple[1], 1, termTTF.get(tuple[0]), tuple[2]);      // docid, tf, ttf, positions
							termDocBlock.get(tuple[0]).put(tuple[1], dBlock);
						}
					}else{
						//termid, docid, position
						DBlock dBlock = new DBlock(tuple[1], 1, termTTF.get(tuple[0]), tuple[2]);
						LinkedHashMap<String, DBlock> docDblockMap = new LinkedHashMap<String, DBlock>();
						docDblockMap.put(tuple[1], dBlock);
						termDocBlock.put(tuple[0], docDblockMap);
					}
				}
			}
		}
		// term and dblock map is now available
		return 	createTermDblockFile(termOffsetMap, termDocBlock);
	}


/**
 * Creates the partial term, DBlock inverted index and returns the updated term, offset map
 * @param termOffsetMap  a map between term and its corresponding offset positions 
 *                       in the inverted index file
 * @param termDocBlock   a map that contains a string and another map that represents
 * 						 term and its DBlock
 * @return               term and offset map for a partial inverted index
 */
	private Map<String, ArrayList<Integer>> createTermDblockFile(Map<String, ArrayList<Integer>> termOffsetMap,	LinkedHashMap<String, LinkedHashMap<String, DBlock>> termDocBlock) {
		try{
			File invertedIndexFile = new File(invertedIndexFileLocation+"invertedIndex.txt");
			if(!invertedIndexFile.exists()){
				invertedIndexFile.createNewFile();
			}
			ArrayList<Integer> positions;
			FileWriter fw = new FileWriter(invertedIndexFile.getAbsoluteFile(), true);
			BufferedWriter addToIndex = new BufferedWriter(fw) ;
			StringBuilder record = new StringBuilder();
			for(Map.Entry<String, LinkedHashMap<String, DBlock>> termBlocks : termDocBlock.entrySet()){  //termBlocks = (termid, <docid, dblock>)
				record.append(termBlocks.getKey()+":");
				for(Map.Entry<String, DBlock> dBlocks : termBlocks.getValue().entrySet()){
					record.append(dBlocks.getValue().getDocID()+" ");
					record.append(String.valueOf(dBlocks.getValue().getTF()+" "));
//					record.append(String.valueOf(dBlocks.getValue().getTTF()+" "));
					record.append(dBlocks.getValue().getPositions().toString().trim()+"#");
				}
				record.append("\n");
				positions = termOffsetMap.get(termBlocks.getKey());
				positions.add(lineBeginning);
				termOffsetMap.put(termBlocks.getKey(), positions);
				lineBeginning += record.length();
				addToIndex.append(record);
				record.setLength(0);
			}
			addToIndex.close();
			fw.close();
		}catch(IOException e){
			e.printStackTrace();
		}
		return termOffsetMap;
	}




/**
 * Converts a normal document to a tuple document
 * @param file  The file that needs to be transformed from normal document
 *              to tuple form
 *              Tuple - (term_id, doc_id, position)
 */
	public void createTupleFile(File file){
		File tupleFile = new File(localUrl+file.getName()+"TUPLE"+".txt");

		// if file doesnt exists, then create it
		try {
			if (!tupleFile.exists()) {
				tupleFile.createNewFile();
			}
			FileWriter fw = new FileWriter(tupleFile.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);

			Indexer indexer = new Indexer();
			BufferedReader br = new BufferedReader(new FileReader(file));
			String currentLine;
			String docID = null;
			StringBuilder sb = new StringBuilder();
			while((currentLine = br.readLine())!=null){
				if(currentLine.contains("<DOCNO>")){
					docID = StringUtils.substringBetween(currentLine, "<DOCNO>", "</DOCNO>");
					docIdMap.put(documentID, docID);
				}
				if(currentLine.contains("<TEXT>")){
					currentLine = br.readLine();
					while(!currentLine.contains("</TEXT>"))
					{
						sb.append(currentLine+" ");
						currentLine = br.readLine();
					}
				}
				if(currentLine.contains("</DOC>")){
					indexer.generateTokens(sb.toString(), bw);
					sb.setLength(0);
				}
			}
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}



	/**
	 * Converts the given docText to a tokenized form there by creating
	 * a tuple file out of the given normal file
	 * @param docText  The text that needs to be tokenized
	 * @param bw       Writing the tokenized data to a file
	 */
	public void generateTokens(String docText, BufferedWriter bw){
		{
			try{
				Stemmer s = new Stemmer();
				Pattern pattern = Pattern.compile("\\w+(\\.?\\w+)*");
				Matcher matcher = pattern.matcher(docText.toLowerCase());
				int positionID = 1;
				StringBuilder content = new StringBuilder();
				while (matcher.find()) {
					for (int i = 0; i < matcher.groupCount(); i++) {
						String term = matcher.group(i);
						if(!stopWords.containsKey(term)){
							if(term.length() > 2){
								s.add(term.toCharArray(), term.toCharArray().length);
								s.stem();
								term = s.toString();
								if(!termIdMap.containsKey(term)){
									termIdMap.put(term, String.valueOf(termIndex));
									termTTF.put(String.valueOf(termIndex), 1);
									termIndex++;
								}else{
									int val = termTTF.get(termIdMap.get(term));
									val = val + 1;
									termTTF.put(termIdMap.get(term), val);
								}
								content.append(String.valueOf(termIdMap.get(term)));
								content.append(" "+String.valueOf(documentID));
								content.append(" "+String.valueOf(positionID)+"#");
								bw.append(content);
								content.setLength(0);
								positionID++;
							}
						}
					}
				}
				bw.append("\n");
				docLenMap.put(documentID, (positionID-1));
				documentID++;
			}catch(IOException e){
				e.printStackTrace();
			}
		}

	}

	
}



