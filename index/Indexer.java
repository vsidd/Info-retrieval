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

	/**
	 * @param args
	 */
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

	public static void main(String[] args) {
		Indexer indexer = new Indexer();
		try {
			URL url = Indexer.class.getResource("/resources/AP_DATA/ap89_collection/"); 
			//URL url = Indexer.class.getResource("/resources/AP_DATA/test/"); 
			//			System.out.println(url);
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
				//				System.out.println("folder"+folder.length());
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


			//			System.out.println(url);
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
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

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
							//							System.out.println("Inverted index created");
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

	private void createCatalogAndFullIndex(Map<String, ArrayList<Integer>> termOffsetMap) {
		try{
			System.out.println("Creating the catalog and final index files");
			long indexLineOffset = 0;
			//			File invertedIndexFile = new File(invertedIndexFileLocation+"invertedIndex.txt");
			//			if(!invertedIndexFile.exists()){
			//				throw new FileNotFoundException("Inverted index not found");
			//			}

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
			String holder;
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
						//						System.out.println(tuple[0]);   //termid, docid, position
						//						System.out.println(tuple[1]);
						//						System.out.println(tuple[2]);
						//						System.out.println(termTTF.get(tuple[0]));
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



	private Map<String, ArrayList<Integer>> createTermDblockFile(Map<String, ArrayList<Integer>> termOffsetMap,	LinkedHashMap<String, LinkedHashMap<String, DBlock>> termDocBlock) {
		try{
			//			System.out.println("Creating inverted index");
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

	//	public void createTermDblockFile(Map<Map.Entry<String, String>, DBlock> termDblock){
	//		try{
	//			// file setup for inverted index
	//			File invertedIndexFile = new File(invertedIndexFileLocation+"invertedIndex.txt");
	//			//			boolean firstTime = false;
	//			if(!invertedIndexFile.exists()){
	//				invertedIndexFile.createNewFile();
	//				//				firstTime = true;
	//			}
	//			FileWriter fw = new FileWriter(invertedIndexFile.getAbsoluteFile(), true);
	//			BufferedWriter addToIndex = new BufferedWriter(fw) ;
	//
	//			// file setup for catalog
	//			File catalogFile = new File(invertedIndexFileLocation+"catalog.txt");
	//			if(!catalogFile.exists()){
	//				catalogFile.createNewFile();
	//			}
	//			FileWriter fw2 = new FileWriter(catalogFile.getAbsoluteFile(), true);
	//			BufferedWriter addToCatalog = new BufferedWriter(fw2) ;
	//
	//			String prev;
	//			StringBuilder content = new StringBuilder();
	//			//get first term separately 
	//			Iterator<Map.Entry<Map.Entry<String, String>, DBlock>> iterator = termDblock.entrySet().iterator();
	//			//			System.out.println("IM OUTSIDE");
	//			if(iterator.hasNext()){
	//				//				System.out.println("IM INSIDE");
	//				Map.Entry<Map.Entry<String, String>, DBlock> temp = iterator.next();
	//				prev = temp.getKey().getKey();
	//				content.append(String.valueOf(prev+" : "));
	//				//***********************************************************
	//				for(Map.Entry<Map.Entry<String, String>, DBlock> termBlock : termDblock.entrySet()){
	//					String term = termBlock.getKey().getKey();
	//					String current = term;
	//					//					System.out.println("prev : "+prev+" current : "+current);
	//					if(!prev.equals(current)){
	//						addToIndex.append(content);
	//						//						System.out.println("content : "+content);
	//						content.setLength(0);
	//						content.append("\n"+term+" : ");
	//						addToCatalog.append(prev+" "+(invertedIndexFile.length()-1)+"\n");
	//					}
	//					DBlock dBlock = termBlock.getValue();
	//					content.append(dBlock.getDocID()+" ");
	//					content.append(String.valueOf(dBlock.getTF()+" "));
	//					content.append(String.valueOf(dBlock.getTTF()+" "));
	//					content.append(dBlock.getPositions().toString()+"#");
	//					prev = current;
	//				}
	//				addToIndex.append(content);
	//				content.setLength(0);
	//				addToCatalog.append(prev+" "+(invertedIndexFile.length()-1)+"\n");
	//				addToCatalog.close();
	//				addToIndex.close();
	//				fw.close();
	//				fw2.close();
	//			}
	//		}catch(IOException e){
	//			e.printStackTrace();
	//		}
	//	}




	//	public Map<Map.Entry<String, String>, DBlock> createInvertedIndex(Map<String, Integer> tempMap){
	//		Map<Map.Entry<String, String>, DBlock> termDBlockMap = new LinkedHashMap<Map.Entry<String, String>, DBlock>();
	//		try{
	//			File tokenDirectory = new File(localUrl);
	//			StringBuilder positions = new StringBuilder();
	//			int termID;
	//			int tf;
	//			String docID;
	//			LinkedHashMap<String, String> termDocMap = new LinkedHashMap<String, String>();
	//						int tcount = 1;
	//			File[] files = tokenDirectory.listFiles();
	//			for(Map.Entry<String, Integer> termIdMapContent : tempMap.entrySet()){  // per term
	//								System.out.println("term count : "+tcount);
	//								tcount++;
	//				for (int i = 0; i < files.length; i++) {                            // loop over all files
	//					File file = files[i];
	//					if(file.isFile())
	//					{
	//						BufferedReader br = new BufferedReader(new FileReader(file));
	//						String currentLine;
	//						while((currentLine = br.readLine())!= null){
	//							//							System.out.println("CURRENT LINE : "+currentLine);
	//							termID = termIdMapContent.getValue();
	//							String[] tuples = currentLine.split("#");
	//							tf = 0;
	//							docID ="";
	//							for(int j = 0; j<tuples.length;j++){
	//								if(tuples[j].length() >1){
	//									String[] tuple = currentLine.split(" ");
	//									//									System.out.println("termID : "+termID+" tuple[0] :"+tuple[0]);
	//									docID = tuple[1];
	//									if(tuple[0].equals(String.valueOf(termID))){                     // optimize here
	//										tf++;
	////										System.out.println("TF : "+tf);
	//										positions.append(tuple[2]+" ");
	//									}
	//								}
	//							}
	//							if(tf>0){
	////								LinkedHashMap<String, String> termDocMap = new LinkedHashMap<String, String>(); // will LinkedHashMap obj good for LinkedHashMap.put() call
	//								termDocMap.put(String.valueOf(termIdMapContent.getValue()), docID);
	//								DBlock dBlock = new DBlock(docID, tf,termTTF.get(termIdMapContent.getValue()), positions);
	//								positions.setLength(0);
	//								for(Map.Entry<String, String> termDocMapTemp : termDocMap.entrySet()){
	//									termDBlockMap.put(termDocMapTemp, dBlock);
	//									termDBlockMap.clear();
	//									break;
	//								}
	//							}
	//						}
	//					}
	//				}
	//			}
	//		}catch(IOException e){
	//			e.printStackTrace();
	//		}
	//		return termDBlockMap;
	//	}




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
					//					Document document = new Document(docID.trim(), sb);
					//					docList.add(document);
					indexer.generateTokens(sb.toString(), bw);
					sb.setLength(0);
				}
			}
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}



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
								//					int startIndex = matcher.start();
								//					int endIndex = matcher.end();
								//					System.out.println(termIndex + " " + term + " " + startIndex
								//							+ " " + endIndex);
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

	class DBlock{
		String docID;
		int tf;
		int ttf;
		StringBuilder positions = new StringBuilder();

		DBlock(){
			this.docID = "";
			this.tf = 0;
			this.ttf = 0;
			this.positions = new StringBuilder();
		}

		//		DBlock(String docID, int tf, int ttf, StringBuilder positions){
		//			this.docID = docID;
		//			this.tf += 1;
		//			this.ttf = ttf;
		//			this.positions = positions;
		//		}

		DBlock(String docID, int tf, int ttf, String position){
			this.docID = docID;
			this.tf = tf;
			this.ttf = ttf;
			this.positions.append(position+" ");
		}

		void update(String position){
			this.tf += 1;
			this.positions.append(position+" ");
		}

		String getDocID(){
			return docID;
		}
		int getTF(){
			return tf;
		}
		int getTTF(){
			return ttf;
		}
		StringBuilder getPositions(){
			return positions;
		}

		void setDocID(String docID){
			this.docID = docID;
		}
		void setPositions(StringBuilder positions){
			this.positions = positions;
		}
		void setTF(int tf){
			this.tf = tf;
		}
		void setTTF(int ttf){
			this.ttf = ttf;
		}
	}
}



//public Map<String, DBlock> createInvertedIndex(Map<String, DBlock> tempMap){
//try{
//File tokenDirectory = new File(localUrl);
//File[] files = tokenDirectory.listFiles();
//for(int i = 0; i< files.length; i++){
//	File file = files[i];
//	if(file.isFile()){
//		BufferedReader br = new BufferedReader(new FileReader(file));
//		String currentLine;
//		while((currentLine = br.readLine())!= null){
//			String[] tuples = currentLine.split("#");
//			for(int j = 0; j<tuples.length; j++){
//				if(tuples[j].length() > 1){
//					String[] tuple = tuples[j].split(" ");
//					if(tempMap.containsKey(tuple[0])){
//						
//					}
//				}
//			}
//		}
//	}
//}
//}catch(IOException e){
//e.printStackTrace();
//}
//return null;
//}
//