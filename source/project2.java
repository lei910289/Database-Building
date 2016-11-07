import java.io.*;
import java.util.*;

import javax.management.Query;

public class project2 {
	static Map<Integer, Long> id = new TreeMap<>();
	static Map<String, ArrayList<Long>> company = new TreeMap<>();
	static Map<String, ArrayList<Long>> drug_id = new TreeMap<>();
	static Map<Short, ArrayList<Long>> trials = new TreeMap<>();
	static Map<Short, ArrayList<Long>> patients = new TreeMap<>();
	static Map<Short, ArrayList<Long>> dosage_mg = new TreeMap<>();
	static Map<Float, ArrayList<Long>> reading = new TreeMap<>();
	static Map<Boolean, ArrayList<Long>> double_blind = new TreeMap<>();
	static Map<Boolean, ArrayList<Long>> controlled_study = new TreeMap<>();
	static Map<Boolean, ArrayList<Long>> govt_funded = new TreeMap<>();
	static Map<Boolean, ArrayList<Long>> fda_approved = new TreeMap<>();
	static ArrayList<Long> offset = null;
	
	
	public static void main(String[] args) {
		boolean isLoop = true;
		while(isLoop){
			System.out.println("---------------------Welcome------------------------");
			runDatabase();
			System.out.println("Want continue?(y/n)");
			Scanner sc = new Scanner(System.in);
			char con = sc.next(".").charAt(0);
			if(con == 'n'){
				isLoop = false;
			}
		}
		System.out.println("------------------------Bye Bye!-------------------");
		
	}

	public static void runDatabase() {
		try {
			// Display all the possible commands into four categories
			System.out.println("Available Functions:  ");
			System.out.println("1.Parse and Create files");
			System.out.println("2.Query");
			System.out.println("3.Insert");
			System.out.println("4.Delete");
			System.out.print("Please select your input : ");

			Scanner scanner = new Scanner(System.in);
			int input = scanner.nextInt();
			RandomAccessFile inputFile = new RandomAccessFile("PHARMA_TRIALS_1000B.csv", "rw");
			// import csv file and create the index file
			if (input == 1) {
				System.out.println("----------------------");
				parse(inputFile);

			}
			else if (input == 2) {
				System.out.println("----------------------");
				System.out.println("You can use either of NOT, =, >, >=, <, <= symbols !");
				System.out.println("Please do not ignore space!");
				System.out.println("Please type your query like:");
				System.out.println("SELECT FROM data.db WHERE id <= 10");
				
				Scanner scanner2 = new Scanner(System.in);
				String deleteCommand = scanner2.nextLine();
				if (deleteCommand.startsWith("SELECT FROM data.db WHERE ")) {
					String line;
					String item[] = new String[2];
					// split six space into 7 items
					String[] record = deleteCommand.split(" ", 7);
					String attr = record[4].trim();
					String sig = record[5].trim();
					String value = record[6].trim();
					int i = 0;
					long offsetNum = 0;
					RandomAccessFile ndxFile = new RandomAccessFile("PHARMA_TRIALS_1000B." + attr + ".ndx", "rw");
					RandomAccessFile dbFile = new RandomAccessFile("data.db", "rw");
					while ((line = ndxFile.readLine()) != null) {
						item = line.split("\t");
						if (sig.equalsIgnoreCase("NOT")) {
							if (!item[0].equals(value)) {
								search(dbFile,item);
							}
						} else if (sig.equalsIgnoreCase("=")) {
							if (item[0].equals(value)) {
								search(dbFile,item);
							}
						} else if (sig.equalsIgnoreCase(">")) {
							if (Integer.parseInt(item[0]) > Integer.parseInt(value)) {
								search(dbFile,item);
							}
						} else if (sig.equalsIgnoreCase(">=")) {
							if (Integer.parseInt(item[0]) >= Integer.parseInt(value)) {
								search(dbFile,item);
							}
						} else if (sig.equalsIgnoreCase("<")) {
							if (Integer.parseInt(item[0]) < Integer.parseInt(value)) {
								search(dbFile,item);
							}
						} else if (sig.equalsIgnoreCase("<=")) {
							if (Integer.parseInt(item[0]) <= Integer.parseInt(value)) {
								search(dbFile,item);
							}
						} else {
							System.out.println("Please using only NOT, =, >, >=, <, <= symbols !");
						}

					}
					dbFile.close();
					ndxFile.close();
				}
			} 

			// insert a new record into db file
			else if (input == 3) {
				System.out.println("----------------------");
				System.out.println("Please type the insert query starting from 1001 like:");
				System.out.println("INSERT INTO data.db VALUES (1001,AAA,aa-111,1,11,111,11.1,TRUE,FALSE,TRUE,FALSE)");
				Scanner scanner2 = new Scanner(System.in);
				String insertQuery = scanner2.nextLine();
				if (insertQuery.startsWith("INSERT INTO data.db VALUES ")) {
					// Use substring to take the attribute values in the query
					String values = insertQuery.substring(28, insertQuery.length() - 1);
					// System.out.println(values);
					insert(values);
					createidIndexFile(id, "PHARMA_TRIALS_1000B.id.ndx");
					createIndexFile(company, "PHARMA_TRIALS_1000B.company.ndx");
					createIndexFile(drug_id, "PHARMA_TRIALS_1000B.drug_id.ndx");
					createIndexFile(trials, "PHARMA_TRIALS_1000B.trials.ndx");
					createIndexFile(patients, "PHARMA_TRIALS_1000B.patients.ndx");
					createIndexFile(dosage_mg, "PHARMA_TRIALS_1000B.dosage_mg.ndx");
					createIndexFile(reading, "PHARMA_TRIALS_1000B.reading.ndx");
					createIndexFile(double_blind, "PHARMA_TRIALS_1000B.double_blind.ndx");
					createIndexFile(controlled_study, "PHARMA_TRIALS_1000B.controlled_study.ndx");
					createIndexFile(govt_funded, "PHARMA_TRIALS_1000B.govt_funded.ndx");
					createIndexFile(fda_approved, "PHARMA_TRIALS_1000B.fda_approved.ndx");
				} else {
					System.out.println("Invalid query!");
				}
			}

			// delete an item from db file
			else if (input == 4) {
				System.out.println("----------------------");
				System.out.println("You can use either of NOT, =, >, >=, <, <= symbols !");
				System.out.println("Please do not ignore space!");
				System.out.println("Please type the delete query like:"); 
				System.out.println("DELETE FROM data.db WHERE id = 1001");
				Scanner scanner2 = new Scanner(System.in);
				String deleteCommand = scanner2.nextLine();
				if (deleteCommand.startsWith("DELETE FROM data.db WHERE ")) {
					String line;
					String item[] = new String[2];
					// split six space into 7 items
					String[] record = deleteCommand.split(" ", 7);
					String attr = record[4].trim();
					String sig = record[5].trim();
					String value = record[6].trim();
					int i = 0;
					long offsetNum = 0;
					// System.out.println("attr is : " +attr +", value is : " +
					// value +" compare is : " + compare);

					RandomAccessFile ndxFile = new RandomAccessFile("PHARMA_TRIALS_1000B." + attr + ".ndx", "rw");
					RandomAccessFile dbFile = new RandomAccessFile("data.db", "rw");
					// Based on the comparison sigal to display all the info in the user's query
					while ((line = ndxFile.readLine()) != null) {
						item = line.split("\t");
						if (sig.equalsIgnoreCase("NOT")) {
							if (!item[0].equals(value)) {
								delete(dbFile, i, offsetNum, item);
							}
						} else if (sig.equalsIgnoreCase("=")) {
							if (item[0].equals(value)) {
								delete(dbFile, i, offsetNum, item);
							}
						} else if (sig.equalsIgnoreCase(">")) {
							if (Integer.parseInt(item[0]) > Integer.parseInt(value)) {
								delete(dbFile, i, offsetNum, item);
							}
						} else if (sig.equalsIgnoreCase(">=")) {
							if (Integer.parseInt(item[0]) >= Integer.parseInt(value)) {
								delete(dbFile, i, offsetNum, item);
							}
						} else if (sig.equalsIgnoreCase("<")) {
							if (Integer.parseInt(item[0]) < Integer.parseInt(value)) {
								delete(dbFile, i, offsetNum, item);
							}
						} else if (sig.equalsIgnoreCase("<=")) {
							if (Integer.parseInt(item[0]) <= Integer.parseInt(value)) {
								delete(dbFile, i, offsetNum, item);
							}
						} else {
							System.out.println("Please using only NOT, =, >, >=, <, <= symbols !");
						}

					}
					dbFile.close();
					ndxFile.close();
				}
			} else {
				System.out.println("Invalid choice");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static void parse(RandomAccessFile inputFile) {

		try {
			String thisLine = inputFile.readLine();
			thisLine = inputFile.readLine();
			long pointer = 0;
			RandomAccessFile dbFile = new RandomAccessFile("data.db", "rw");
			while (thisLine != null) {
				String[] value = thisLine.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
				if (value[0].trim().equalsIgnoreCase("id"))
					continue;
				pointer = dbFile.getFilePointer();
			
				value[1] = value[1].replace("\"", "");
				int var1 = Integer.parseInt(value[0]);
				Short var4 = Short.parseShort(value[3]);
				Short var5 = Short.parseShort(value[4]);
				Short var6 = Short.parseShort(value[5]);
				Float var7 = Float.parseFloat(value[6]);
				Boolean var8 = Boolean.parseBoolean(value[7]);
				Boolean var9 = Boolean.parseBoolean(value[8]);
				Boolean var10 = Boolean.parseBoolean(value[9]);
				Boolean var11 = Boolean.parseBoolean(value[10]);

				id.put(var1, pointer);
				// store in the treemap data structure in a sorted order
				if (company.containsKey(value[1]))
					offset = company.get(value[1]);
				else
					offset = new ArrayList<Long>();
				offset.add(pointer);
				company.put(value[1], offset);

				if (drug_id.containsKey(value[2]))
					offset = drug_id.get(value[2]);
				else
					offset = new ArrayList<Long>();
				offset.add(pointer);
				drug_id.put(value[2], offset);

				if (trials.containsKey(var4))
					offset = trials.get(var4);
				else
					offset = new ArrayList<Long>();
				offset.add(pointer);
				trials.put(var4, offset);

				if (patients.containsKey(var5))
					offset = patients.get(var5);
				else
					offset = new ArrayList<Long>();
				offset.add(pointer);
				patients.put(var5, offset);

				if (dosage_mg.containsKey(var6))
					offset = dosage_mg.get(var6);
				else
					offset = new ArrayList<Long>();
				offset.add(pointer);
				dosage_mg.put(var6, offset);

				if (reading.containsKey(var7))
					offset = reading.get(var7);
				else
					offset = new ArrayList<Long>();
				offset.add(pointer);
				reading.put(var7, offset);

				if (double_blind.containsKey(var8))
					offset = double_blind.get(var8);
				else
					offset = new ArrayList<Long>();
				offset.add(pointer);
				double_blind.put(var8, offset);

				if (controlled_study.containsKey(var9))
					offset = controlled_study.get(var9);
				else
					offset = new ArrayList<Long>();
				offset.add(pointer);
				controlled_study.put(var9, offset);

				if (govt_funded.containsKey(var10))
					offset = govt_funded.get(var10);
				else
					offset = new ArrayList<Long>();
				offset.add(pointer);
				govt_funded.put(var10, offset);

				if (fda_approved.containsKey(var11))
					offset = fda_approved.get(var11);
				else
					offset = new ArrayList<Long>();
				offset.add(pointer);
				fda_approved.put(var11, offset);

				dbFile.writeInt(Integer.parseInt(value[0]));
				dbFile.writeByte(value[1].length());
				dbFile.writeBytes(value[1]);
				dbFile.writeBytes(value[2]);
				dbFile.writeShort(Short.parseShort(value[3]));
				dbFile.writeShort(Short.parseShort(value[4]));
				dbFile.writeShort(Short.parseShort(value[5]));
				dbFile.writeFloat(Float.parseFloat(value[6]));
				// set the initial delete bit to 0
				Byte commonByte = (byte) 00000000;
				if (Boolean.parseBoolean(value[7]))
					commonByte = (byte) (commonByte | (1 << 3));
				if (Boolean.parseBoolean(value[8]))
					commonByte = (byte) (commonByte | (1 << 2));
				if (Boolean.parseBoolean(value[9]))
					commonByte = (byte) (commonByte | (1 << 1));
				if (Boolean.parseBoolean(value[10]))
					commonByte = (byte) (commonByte | (1 << 0));
				dbFile.writeByte(commonByte);
				thisLine = inputFile.readLine();
			}
			dbFile.close();

			id = new TreeMap<>(id);
			company = new TreeMap<>(company);
			drug_id = new TreeMap<>(drug_id);
			trials = new TreeMap<>(trials);
			patients = new TreeMap<>(patients);
			dosage_mg = new TreeMap<>(dosage_mg);
			reading = new TreeMap<>(reading);
			double_blind = new TreeMap<>(double_blind);
			controlled_study = new TreeMap<>(controlled_study);
			govt_funded = new TreeMap<>(govt_funded);
			fda_approved = new TreeMap<>(fda_approved);

			createidIndexFile(id, "PHARMA_TRIALS_1000B.id.ndx");
			createIndexFile(company, "PHARMA_TRIALS_1000B.company.ndx");
			createIndexFile(drug_id, "PHARMA_TRIALS_1000B.drug_id.ndx");
			createIndexFile(trials, "PHARMA_TRIALS_1000B.trials.ndx");
			createIndexFile(patients, "PHARMA_TRIALS_1000B.patients.ndx");
			createIndexFile(dosage_mg, "PHARMA_TRIALS_1000B.dosage_mg.ndx");
			createIndexFile(reading, "PHARMA_TRIALS_1000B.reading.ndx");
			createIndexFile(double_blind, "PHARMA_TRIALS_1000B.double_blind.ndx");
			createIndexFile(controlled_study, "PHARMA_TRIALS_1000B.controlled_study.ndx");
			createIndexFile(govt_funded, "PHARMA_TRIALS_1000B.govt_funded.ndx");
			createIndexFile(fda_approved, "PHARMA_TRIALS_1000B.fda_approved.ndx");
			System.out.println("Import completed!");
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	// create the index file for id attribute
	public static void createidIndexFile(Map<Integer, Long> map, String indexFile) {
		try {
			FileOutputStream out = new FileOutputStream(indexFile);
			PrintWriter pw = new PrintWriter(out);
			for (Map.Entry<Integer, Long> entry : map.entrySet()) {
				pw.println(String.valueOf(entry.getKey()) + "\t" + String.valueOf(entry.getValue()));
			}
			pw.flush();
			pw.close();
			out.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// create the index file for other attributes of different data structure
	public static <T> void createIndexFile(Map<T, ArrayList<Long>> map2, String indexFile) {
		try {
			FileWriter fw = new FileWriter(indexFile);
			Writer output = new BufferedWriter(fw);
			for (Map.Entry<T, ArrayList<Long>> entry : map2.entrySet()) {
				output.write(String.valueOf(entry.getKey()) + "\t");
				int size = entry.getValue().size();
				for (int i = 0; i < size - 1; i++)
					output.write(entry.getValue().get(i).toString() + ",");
				output.write(entry.getValue().get(size - 1).toString());
				output.write("\n");
			}
			output.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void insert(String insertValue) {
		try {
			
			RandomAccessFile dbFile = new RandomAccessFile("data.db", "rw");
			// take each item from the user's input of insert query and put into
			// the data array for splitting
			String[] value = insertValue.split("\"?(,|$)(?=(([^\"]*\"){2})*[^\"]*$) *\"?");

			long pointer = dbFile.getFilePointer() + dbFile.length() - 1;
			// move the pointer for selecting needed attr
			dbFile.seek(pointer);
			int var1 = Integer.parseInt(value[0]);
			Short var4 = Short.parseShort(value[3]);
			Short var5 = Short.parseShort(value[4]);
			Short var6 = Short.parseShort(value[5]);
			Float var7 = Float.parseFloat(value[6]);
			Boolean var8 = Boolean.parseBoolean(value[7]);
			Boolean var9 = Boolean.parseBoolean(value[8]);
			Boolean var10 = Boolean.parseBoolean(value[9]);
			Boolean var11 = Boolean.parseBoolean(value[10]);

			BufferedWriter bw = new BufferedWriter(new FileWriter("PHARMA_TRIALS_1000B.csv", true));
			bw.write(value[0] + "," + value[1] + "," + value[2] + "," + value[3] + "," + value[4] + "," + value[5] + "," + value[6] + "," + value[7]
					+ "," + value[8] + "," + value[9] + "," + value[10]);
			bw.newLine();
			bw.close();
			id.put(var1, pointer);

			if (company.containsKey(value[1])) {
				offset = company.get(value[1]);
			} else {
				offset = new ArrayList<Long>();
			}
			offset.add(pointer);
			company.put(value[1], offset);

			if (drug_id.containsKey(value[2])) {
				offset = drug_id.get(value[2]);
			} else {
				offset = new ArrayList<Long>();
			}
			offset.add(pointer);
			drug_id.put(value[2], offset);

			if (trials.containsKey(var4)) {
				offset = trials.get(var4);
			} else {
				offset = new ArrayList<Long>();
			}
			offset.add(pointer);
			trials.put(var4, offset);

			if (patients.containsKey(var5)) {
				offset = patients.get(var5);
			} else {
				offset = new ArrayList<Long>();
			}
			offset.add(pointer);
			patients.put(var5, offset);

			if (dosage_mg.containsKey(var6)) {
				offset = dosage_mg.get(var6);
			} else {
				offset = new ArrayList<Long>();
			}
			offset.add(pointer);
			dosage_mg.put(var6, offset);

			if (reading.containsKey(var7)) {
				offset = reading.get(var7);
			} else {
				offset = new ArrayList<Long>();
			}
			offset.add(pointer);
			reading.put(var7, offset);

			if (double_blind.containsKey(var8)) {
				offset = double_blind.get(var8);
			} else {
				offset = new ArrayList<Long>();
			}
			offset.add(pointer);
			double_blind.put(var8, offset);

			if (controlled_study.containsKey(var9)) {
				offset = controlled_study.get(var9);
			} else {
				offset = new ArrayList<Long>();
			}
			offset.add(pointer);
			controlled_study.put(var9, offset);

			if (govt_funded.containsKey(var10)) {
				offset = govt_funded.get(var10);
			} else {
				offset = new ArrayList<Long>();
			}
			offset.add(pointer);
			govt_funded.put(var10, offset);

			if (fda_approved.containsKey(var11)) {
				offset = fda_approved.get(var11);
			} else {
				offset = new ArrayList<Long>();
			}
			offset.add(pointer);
			fda_approved.put(var11, offset);

			dbFile.writeInt(Integer.parseInt(value[0]));
			dbFile.writeByte(value[1].length());
			dbFile.writeBytes(value[1]);
			dbFile.writeBytes(value[2]);
			dbFile.writeShort(Short.parseShort(value[3]));
			dbFile.writeShort(Short.parseShort(value[4]));
			dbFile.writeShort(Short.parseShort(value[5]));
			dbFile.writeFloat(Float.parseFloat(value[6]));
			// set the initial delete bit to 0
			Byte commonByte = (byte) 00000000;
			if (Boolean.parseBoolean(value[7]))
				commonByte = (byte) (commonByte | (1 << 3));
			if (Boolean.parseBoolean(value[8]))
				commonByte = (byte) (commonByte | (1 << 2));
			if (Boolean.parseBoolean(value[9]))
				commonByte = (byte) (commonByte | (1 << 1));
			if (Boolean.parseBoolean(value[10]))
				commonByte = (byte) (commonByte | (1 << 0));
			dbFile.writeByte(commonByte);

			dbFile.writeBytes("\n");
			dbFile.close();

			id = new TreeMap<>(id);
			company = new TreeMap<>(company);
			drug_id = new TreeMap<>(drug_id);
			trials = new TreeMap<>(trials);
			patients = new TreeMap<>(patients);
			dosage_mg = new TreeMap<>(dosage_mg);
			reading = new TreeMap<>(reading);
			double_blind = new TreeMap<>(double_blind);
			controlled_study = new TreeMap<>(controlled_study);
			govt_funded = new TreeMap<>(govt_funded);
			fda_approved = new TreeMap<>(fda_approved);
			System.out.println("Insert command executed!");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void tableQuery(String field, String value) {
		// use flag to indicate weather the data has been deleted or has never
		// existed.
//		boolean flagExist = false;
		try {
			long position =0;
			RandomAccessFile indexFile = new RandomAccessFile("PHARMA_TRIALS_1000B." + field + ".ndx", "r");
			RandomAccessFile dbFile = new RandomAccessFile("data.db", "r");
			String thisLine;
			String[] index = new String[2];

			while ((thisLine = indexFile.readLine()) != null) {
				index = thisLine.split("\t");
				if (value.equals(index[0])) {
//					search(dbFile,position,index);
				}
			}
			indexFile.close();
			dbFile.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

//		if (flagExist == false) {
//			System.out.println("The data you are querying does not exist!");
//		}
	}
	
	public static void search(RandomAccessFile dbFile,String[] index ) throws IOException{
		long position =0;
		//String[] index = new String[2];
//		flagExist = true;
//		System.out.println("flag1");
		String[] pos = index[1].split(",");
		for (int i = pos.length - 1; i >= 0; i--) {
			position = Long.parseLong(pos[i]);
			dbFile.seek(position);
			int outid = dbFile.readInt();

			int length = dbFile.read();
			byte[] companyByte = new byte[length];
			dbFile.read(companyByte);
			String outcompany = new String(companyByte);

			byte[] drug_idByte = new byte[6];
			dbFile.read(drug_idByte);
			String outdrug_id = new String(drug_idByte);

			short outtrials = dbFile.readShort();
			short outpatients = dbFile.readShort();
			short outdosage_mg = dbFile.readShort();
			float outreading = dbFile.readFloat();

			int commonByte = dbFile.read();
			String outdouble_blind, outcontrolled_study, outgovt_funded, outfda_approved;
			if ((commonByte & 8) == 8)
				outdouble_blind = "true";
			else
				outdouble_blind = "false";
			if ((commonByte & 4) == 4)
				outcontrolled_study = "true";
			else
				outcontrolled_study = "false";
			if ((commonByte & 2) == 2)
				outgovt_funded = "true";
			else
				outgovt_funded = "false";
			if ((commonByte & 1) == 1)
				outfda_approved = "true";
			else
				outfda_approved = "false";

			if ((commonByte >> 7) != 1) {
				System.out.println(outid + "\t" + outcompany + "\t" + outdrug_id + "\t" + outtrials + "\t" + outpatients + "\t"
						+ outdosage_mg + "\t" + outreading + "\t" + outdouble_blind + "\t" + outcontrolled_study + "\t" + outgovt_funded
						+ "\t" + outfda_approved + "\t");
			} else if ((commonByte >> 7) == 1) {
				System.out.println("There is no such data in the database!");
			} else {

			}

		}
	
	}

	public static void delete(RandomAccessFile dbFile, int i, long offsetNumber, String item[]) throws IOException {

		String offset[] = item[1].split(",");
		for (i = offset.length - 1; i >= 0; i--) {
			offsetNumber = Long.parseLong(offset[i]);
			dbFile.seek(offsetNumber);
			int id = dbFile.readInt();
			int length = dbFile.read();
			byte[] comp = new byte[length];
			dbFile.read(comp);
			byte[] drugid = new byte[6];
			dbFile.read(drugid);
			dbFile.readShort();
			dbFile.readShort();
			dbFile.readShort();
			dbFile.readFloat();
			// set the initial delete bit 0 to 1 indicating the data has been
			// delected
			Byte byt = (byte) 10000000;
			Byte byt2 = dbFile.readByte();
			byt2 = (byte) (byt2 | byt);
			dbFile.seek(offsetNumber);
			dbFile.readInt();
			length = dbFile.read();
			comp = new byte[length];
			dbFile.read(comp);
			drugid = new byte[6];
			dbFile.read(drugid);
			dbFile.readShort();
			dbFile.readShort();
			dbFile.readShort();
			dbFile.readFloat();
			dbFile.write(byt2);
			System.out.println("Delete command executed!");
		}

	}
}
