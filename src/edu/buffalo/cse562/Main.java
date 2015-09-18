package edu.buffalo.cse562;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import jdbm.InverseHashView;
import jdbm.PrimaryStoreMap;
import jdbm.PrimaryTreeMap;
import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import jdbm.SecondaryKeyExtractor;
import jdbm.SecondaryTreeMap;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.replace.Replace;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;

public class Main implements StatementVisitor{
	
	private HashMap<String, MyTable> tables;
	private String filePath;
	private static long startTime = System.currentTimeMillis();
	private static MyTable outputTable;
	private static ArrayList<Integer> projections;
	private static long limit;
	private File dataDir = null;
	private File swapDir = null;
	private File indexDir = null;
	private HashMap<String, ArrayList<String>> leftoverConditions;

	/** This boolean variable is a flag to show whether we are in preprocess or query run*/
	private static boolean preprocess = false;
	
	public static void main(String[] args) {
		Main m = new Main();
		/*try {
			RecordManager recman = RecordManagerFactory.createRecordManager("index/LINEITEM");
			PrimaryTreeMap<String, MyTuple> index = recman.treeMap("Primary_LINEITEM_orderkey_linenumber");
			System.out.println(index.get(0));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		m.beginParsing(args);
		long endTime = System.currentTimeMillis(); 
		if(!preprocess){  //If we are evaluating queries , not building index
			/* projection */
			/* limit OPTIONAL */
			if(limit == -1){
				/* output result with everything */
				if(projections.size() == 0){
					for(MyTuple t: outputTable.getTuples()){
						int counter = 1;
						for(MyColumn c: t.getTupleList()){
							if(counter < t.getTupleList().size()){
								System.out.print(c.getValue() +"|");
							}else{
								System.out.println(c.getValue());
							}
							counter += 1;
						}
					} 
				 }else{
					/* if there are columns to be projected, project only them */
				 	ProjectionOperator po = new ProjectionOperator(projections, outputTable);
				 	while(po.hasNext()){
						MyTuple nextTuple = po.readOneTuple();
						int counter = 1;
						for(MyColumn c: nextTuple.getTupleList()){
							if(counter < nextTuple.getTupleList().size()){
								System.out.print(c.getValue() +"|");
							}else{
								System.out.println(c.getValue());
							}
							counter += 1;
						}
					}
				 }
			}else{
				/* if there is limit, output correct result */
				/* output result with everything */
				if(projections.size() == 0){
					int outputs = 1;
					for(MyTuple t: outputTable.getTuples()){
						int counter = 1;
						for(MyColumn c: t.getTupleList()){
							if(counter < t.getTupleList().size()){
								System.out.print(c.getValue() +"|");
							}else{
								System.out.println(c.getValue());
							}
							counter += 1;
						}
						if(outputs == limit){
							break;
						}
						outputs++;
					} 
				 }else{
					/* if there are columns to be projected, project only them */
				 	ProjectionOperator po = new ProjectionOperator(projections, outputTable);
				 	int outputs = 1;
				 	while(po.hasNext()){
						MyTuple nextTuple = po.readOneTuple();
						int counter = 1;
						for(MyColumn c: nextTuple.getTupleList()){
							if(counter < nextTuple.getTupleList().size()){
								System.out.print(c.getValue() +"|");
							}else{
								System.out.println(c.getValue());
							}
							counter += 1;
						}
						if(outputs == limit){
							break;
						}
						outputs++;
					}
				 }
			}
		}
		/*System.out.print("Number of Rows in set: " + outputTable.getTuples().size()+ " ");
		if(limit != -1){
			System.out.print("-- Limited to " +limit);
		}
		System.out.println();*/
		//System.out.println("Time: "+ ((endTime - startTime) / 1000.0000) +" sec");
	}
	
    
    public void beginParsing(String[] args){
		/* start reading from the file, in a loop */
	    int i;
	    
	    //File dataDir = null;
	    ArrayList<File> sqlFiles = new ArrayList<File>();
	    tables = new HashMap<String, MyTable>();
	    for(i = 0; i < args.length; i++){
	    	if(args[i].equals("--build"))
    		{
    		    preprocess = true;
    		    i++;
    			//return;
    		}
	    	if (args[i].equals("--data")){
	    		if(!(args[i+1].equals("--swap"))){
	    			dataDir = new File(args[i+1]);
		    		i++;
	    		}
	    	}else if(args[i].equals("--swap")){
	    		if(!(args[i+1].contains(".sql"))){
		    		swapDir = new File(args[i+1]);
		    		i++;
	    		}
	    	}
	    	else if(args[i].equals("--index")){
	    			indexDir = new File(args[i+1]);
		    		i++;
		    			    		
	    	}
	    	else{
	    		sqlFiles.add(new File(args[i]));
	    	
	    	}
	    }
	    
	    
	    for(File sqlFile: sqlFiles){
	    	String fileString = "";
	    	try{
	        	//System.out.println("File: " +sqlFile);
	        	//fileString = sqlFile.getAbsolutePath().substring(0, sqlFile.getAbsolutePath().length());
	        	//filePath = fileString.substring(0, fileString.lastIndexOf(File.separatorChar));
	        	//FileReader stream = new FileReader(new File(fileString));
	        	FileReader stream = new FileReader(sqlFile);
	        	CCJSqlParser parser = new CCJSqlParser(stream);
	        	Statement stmt;
	        	while((stmt = parser.Statement()) != null){
	        		//System.out.println("Statement: " +stmt);
	        		stmt.accept(this);

	        	}
        		if(preprocess)
        		{
        			return;
        		}
	    	}catch(IOException e){
	    		e.printStackTrace();
	    		System.out.println(fileString);
	    		System.out.println(filePath);
	    	}catch(ParseException e){
	    		e.printStackTrace();
	    		System.out.println("SQL parse failed. Check your SQL file's syntax.");
	    	}
	    /* end reading of the file */
	    }
	}

	@Override
	public void visit(Select arg0) {
		//System.out.println("INSTANCE OF SELECT");
		MySelectVisitor sv = new MySelectVisitor(tables, arg0, indexDir);
		outputTable = sv.getOutputTable();
		projections = sv.getProjections();
		limit = sv.getLimit();
	}

	@Override
	public void visit(Delete del) {
		final MyTable table = this.tables.get(del.getTable().toString().toUpperCase());
		String tblName = table.getTablename();
		try {
			RecordManager recman = RecordManagerFactory.createRecordManager(indexDir.toString() + File.separatorChar + table.getTablename());
			PrimaryStoreMap<Long, MyTuple> primaryIn = recman.storeMap("Primary_" +table.getTablename(), 
					new MyTupleSerializer(table.columnList.size(), table));
			InverseHashView<Long, MyTuple> inverseTuple = primaryIn.inverseHashView("TuplesInverse");
			SecondaryTreeMap< String, Long, MyTuple> secondaryIn1 = null;
			SecondaryTreeMap< String, Long, MyTuple> secondaryIn2 = null;
			if(table.getSecondaryIndexes().size() >=1){
				secondaryIn1 = primaryIn.secondaryTreeMap("Secondary_"+tblName+"_"+table.getSecondaryIndexes().get(0), new SecondaryKeyExtractor<String, Long, MyTuple>() {   
				public String extractSecondaryKey(Long key, MyTuple t){
				                             // System.out.println(in.getColumnsNames().get(0).toString().toUpperCase());
				                              int pos= t.getColumnPosition(table.getSecondaryIndexes().get(0).toUpperCase());
				                              return t.getTupleList().get(pos).getValue();
				                              }
			                               });
			}
			if(table.getSecondaryIndexes().size() > 1){
				secondaryIn2 = primaryIn.secondaryTreeMap("Secondary_"+tblName+"_"+table.getSecondaryIndexes().get(1), new SecondaryKeyExtractor<String, Long, MyTuple>() {   
				public String extractSecondaryKey(Long key, MyTuple t){
				                             // System.out.println(in.getColumnsNames().get(0).toString().toUpperCase());
				                              int pos= t.getColumnPosition(table.getSecondaryIndexes().get(1).toUpperCase());
				                              return t.getTupleList().get(pos).getValue();
				                              }
			                               });
			}
			Expression e = del.getWhere();
			MyExpressionVisitor ev = new MyExpressionVisitor(null);

			///////////////////////
			//Original version
			for(MyTuple t: primaryIn.values()){
				// Evaluate the condition on the tuple 
				ev.setTuple(t);
				e.accept(ev);
				if(ev.getBooleanResult()){
					long key = inverseTuple.findKeyForValue(t);
					primaryIn.remove(key);
					//System.out.println("Removed From " +table.getTablename()+ ": " +t);
				}
			}
			
			recman.commit();recman.clearCache();

		
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	@Override
	public void visit(Update upd) {
		final MyTable table = this.tables.get(upd.getTable().toString().toUpperCase());
		try {
			String tblName = table.getTablename();
			RecordManager recman = RecordManagerFactory.createRecordManager(indexDir.toString() + File.separatorChar + table.getTablename());
			PrimaryStoreMap<Long, MyTuple> primaryIn = recman.storeMap("Primary_" +table.getTablename(), 
					new MyTupleSerializer(table.columnList.size(), table));
			InverseHashView<Long, MyTuple> inverseTuple = primaryIn.inverseHashView("TuplesInverse");
			SecondaryTreeMap< String, Long, MyTuple> secondaryIn1 = null;
			SecondaryTreeMap< String, Long, MyTuple> secondaryIn2 = null;
			SecondaryTreeMap<String, Long, MyTuple> secondaryIndex = null;
			if(table.getSecondaryIndexes().size() >=1){
				secondaryIn1 = primaryIn.secondaryTreeMap("Secondary_"+tblName+"_"+table.getSecondaryIndexes().get(0), new SecondaryKeyExtractor<String, Long, MyTuple>() {   
				public String extractSecondaryKey(Long key, MyTuple t){
				                             // System.out.println(in.getColumnsNames().get(0).toString().toUpperCase());
				                              int pos= t.getColumnPosition(table.getSecondaryIndexes().get(0).toUpperCase());
				                              return t.getTupleList().get(pos).getValue();
				                              }
			                               });
			}
			if(table.getSecondaryIndexes().size() > 1){
				secondaryIn2 = primaryIn.secondaryTreeMap("Secondary_"+tblName+"_"+table.getSecondaryIndexes().get(1), new SecondaryKeyExtractor<String, Long, MyTuple>() {   
				public String extractSecondaryKey(Long key, MyTuple t){
				                             // System.out.println(in.getColumnsNames().get(0).toString().toUpperCase());
				                              int pos= t.getColumnPosition(table.getSecondaryIndexes().get(1).toUpperCase());
				                              return t.getTupleList().get(pos).getValue();
				                              }
			                               });
			}
			/* Get the tuples from the index that are to be deleted */
			Expression e = upd.getWhere();
			MyExpressionVisitor ev = new MyExpressionVisitor(null);
			String columnName = upd.getColumns().get(0).toString();
			String newVal = upd.getExpressions().get(0).toString();
			String parsedVal = "";
			if(newVal.contains("date")){
				parsedVal = newVal.replace("date('", "");
				parsedVal = parsedVal.replace("')", "");
			}else{
				parsedVal = newVal.replace("'", "");
			}
			int columnPosition = table.getColumnPosition(columnName);
			/* Check if the table has an index on the where condition */
			String expString = e.toString();
			final String column;
			if(expString.contains(">")){
				column = expString.substring(0, expString.indexOf(">")).trim().toUpperCase();
			}else{
				column = expString.substring(0, expString.indexOf("<")).trim().toUpperCase();
			}
			
			if(table.hasSecondaryIndexOn(column)){// || table.hasConjoinedKey()){
					//System.out.println("Secondary_"+table.getTablename()+"_"+column);
					
					
				IndexScanDetail isd = null;
				if(expString.contains(">")){
					isd = getUpperAndLowerBounds(e.toString());
				}else{
					isd = new IndexScanDetail("0", "2", 
					">", "<", column, e.toString(), null);
			    }
					if(column.equalsIgnoreCase(table.getSecondaryIndexes().get(0)))
					{
						secondaryIndex = secondaryIn1;
					}
					else if(column.equalsIgnoreCase(table.getSecondaryIndexes().get(1)))
					{
						secondaryIndex = secondaryIn2;
					}
/*					else if(column.equalsIgnoreCase(table.getPrimaryIndexes().get(1)))
					{
						secondaryIndex = secPrim2;
					}*/
	
					MyTuple next = null;
					SortedMap<String, Iterable<Long>> reducedMap = secondaryIndex.subMap(
					isd.getLowerBound(), isd.getUpperBound());
					List<Long> deepCopy = new ArrayList<Long>();
				    for(Iterable<Long> tupleKeys: reducedMap.values()){
						deepCopy.addAll((Collection<? extends Long>) tupleKeys);
					}
					    int commitMe = 0;
						for(Long key: deepCopy){
							//System.out.println();
							next = primaryIn.get(key);
							next.getColumn(columnPosition).setValue(parsedVal);
							primaryIn.put(key, next);
							commitMe++;
							if(commitMe == 10000){
								commitMe = 0;
								recman.commit();
								recman.clearCache();
							}

						}
					
					recman.commit();
					recman.clearCache();
					recman.close();
					/* WARNING: we do not need to add or remove tuples because the
					* operators are >= and < */
			}else{
					
				int commitMe = 0;
				for(MyTuple t: primaryIn.values()){
					// Evaluate the condition on the tuple 
					ev.setTuple(t);
					e.accept(ev);
					if(ev.getBooleanResult()){
						long key = inverseTuple.findKeyForValue(t);
						//primaryIn.remove(key);
						t.getColumn(columnPosition).setValue(parsedVal);
						primaryIn.put(key, t);
					
					}
					commitMe++;
					if(commitMe == 10000){
						commitMe = 0;
						recman.commit();
						recman.clearCache();
					}
				}
				recman.commit();
				recman.clearCache();
				recman.close();
			}
			
			} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			}
	}
			
	@Override
	public void visit(Insert ins) {
		try {
			String tblName = ins.getTable().toString();
			RecordManager recman = RecordManagerFactory.createRecordManager(indexDir.toString() + File.separatorChar + tblName);
			final MyTable tbl = tables.get(tblName);
			PrimaryStoreMap<Long, MyTuple> primaryIn = recman.storeMap("Primary_"+ tblName, new MyTupleSerializer(tbl.columnList.size(), tbl));
			String newStringTuple = ins.getItemsList().toString();
			InverseHashView<Long, MyTuple> inverseTuple = primaryIn.inverseHashView("TuplesInverse");
			SecondaryTreeMap< String, Long, MyTuple> secondaryIn2  = null;
			SecondaryTreeMap< String, Long, MyTuple> secondaryIn1 = null;
			if(tbl.getSecondaryIndexes().size() >=1){
				secondaryIn1 = primaryIn.secondaryTreeMap("Secondary_"+tblName+"_"+tbl.getSecondaryIndexes().get(0), new SecondaryKeyExtractor<String, Long, MyTuple>() {   
				public String extractSecondaryKey(Long key, MyTuple t){
				                             // System.out.println(in.getColumnsNames().get(0).toString().toUpperCase());
				                              int pos= t.getColumnPosition(tbl.getSecondaryIndexes().get(0).toUpperCase());
				                              return t.getTupleList().get(pos).getValue();
				                              }
			                               });
			}
			if(tbl.getSecondaryIndexes().size() > 1){
				 secondaryIn2 = primaryIn.secondaryTreeMap("Secondary_"+tblName+"_"+tbl.getSecondaryIndexes().get(1), new SecondaryKeyExtractor<String, Long, MyTuple>() {   
				public String extractSecondaryKey(Long key, MyTuple t){
				                             // System.out.println(in.getColumnsNames().get(0).toString().toUpperCase());
				                              int pos= t.getColumnPosition(tbl.getSecondaryIndexes().get(1).toUpperCase());
				                              return t.getTupleList().get(pos).getValue();
				                              }
			                               });
			}
						
			if(newStringTuple.startsWith("("))
			{
				newStringTuple = newStringTuple.substring(1, newStringTuple.length()-1);
			}
			newStringTuple = newStringTuple.replaceAll(",", "|");
			newStringTuple = newStringTuple.replaceAll("date\\(\\'", "");
			newStringTuple = newStringTuple.replaceAll("\\'\\)", "");
			newStringTuple = newStringTuple.replaceAll("\\'", "");
			
			MyTuple t = Utility.stringToTuple(newStringTuple, tbl);
			primaryIn.putValue(t);
			recman.commit();
			recman.clearCache();
			recman.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	@Override
	public void visit(Replace arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Drop arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Truncate arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(CreateTable arg0) {
		//System.out.println("INSTANCE OF CREATE");
		
		/* Create the table in memory using an instance of MyTable */
	
		String tableName = arg0.getTable().getName().toUpperCase();
		
		int position = 0;
		HashMap<String, Integer> columnOrder = new HashMap<String, Integer>();
		HashMap<String, String> colTypes = new HashMap<String, String>();
		ArrayList<String> columnList = new ArrayList<String>();
		final ArrayList<String> pind = new ArrayList<String>(); //ArrayList of primary indexes
		ArrayList<String> sind = new ArrayList<String>(); //ArrayList of secondary indexes
		for(Object o: arg0.getColumnDefinitions()){
			String colName = o.toString().substring(0, o.toString().indexOf(" "));
			columnList.add(colName.toUpperCase());
			columnOrder.put(colName.toUpperCase(), position);
			position += 1;
			
			String colType = o.toString().substring(o.toString().indexOf(" ")+1, 
					o.toString().length()-1);
			colTypes.put(colName.toUpperCase(), colType);
		}
		List<Object> indices = arg0.getIndexes();  //List of Primary and secondary indexes in a table
		for(int i=0; i<indices.size(); i++)
		{
			Index in = (Index) indices.get(i); 
			if(in.getType().equalsIgnoreCase("PRIMARY KEY"))
			{
				for(Object o: in.getColumnsNames()){
				     pind.add(o.toString());  //Columns involving in one primary index
				}
			}
			else
			{
				for(Object o: in.getColumnsNames()){
				     sind.add(o.toString());  //Columns involving in one Secondary index
				}
			}
	
		}
		File tableDataFile = new File(dataDir.toString() +File.separatorChar+tableName+ ".dat");
		MyTable newTable = new MyTable(tableDataFile, tableName, columnOrder, 
				colTypes, columnList, pind, sind);

		tables.put(tableName.toUpperCase(), newTable);
		if(preprocess){   //So we are in preprocess phase, we have to create indexes
			
			if(tableName.equalsIgnoreCase("LINEITEM") || tableName.equalsIgnoreCase("ORDERS") || tableName.equalsIgnoreCase("NATION") || tableName.equalsIgnoreCase("CUSTOMER")){
				try {
					
						RecordManager recman = RecordManagerFactory.createRecordManager(indexDir.toString() + File.separatorChar + tableName);
						//PrimaryTreeMap<String, MyTuple> primaryIn = null;
						PrimaryStoreMap<Long, MyTuple> primaryIn = null;
						//PrimaryTreeMap<String, ArrayList<String>> testSec = null;
						SecondaryTreeMap<String, Long, MyTuple> secondaryIn = null;
						//PrimaryStoreMap<Long, ArrayList<String>> testSec = null;
						//This two variables are for cases we have a composite key
						SecondaryTreeMap<String, Long, MyTuple> secPrim1 = null;
						SecondaryTreeMap<String, Long, MyTuple> secPrim2 = null;
						InverseHashView<Long, MyTuple> inverseTuple = null;
						for(int i= 0; i<indices.size(); i++)
						{
							final Index in = (Index) indices.get(i);
							                   /* String indexName = in.getColumnsNames().get(0).toString();
							                    for(int j=1; j<in.getColumnsNames().size(); j++)
							                    {
							                    indexName+="_"+ in.getColumnsNames().get(j).toString();
							                    }*/
							if(in.getType().equalsIgnoreCase("PRIMARY KEY")){
							     primaryIn = recman.storeMap("Primary_"+ tableName, new MyTupleSerializer(columnList.size(), newTable));
							     System.out.println("Primary_"+ tableName);
							     inverseTuple = primaryIn.inverseHashView("TuplesInverse");
							}
							else
							{
								secondaryIn = primaryIn.secondaryTreeMap("Secondary_"+tableName+"_"+in.getColumnsNames().get(0), new SecondaryKeyExtractor<String, Long, MyTuple>() {   
								public String extractSecondaryKey(Long key, MyTuple t){
								                             // System.out.println(in.getColumnsNames().get(0).toString().toUpperCase());
								                              int pos= t.getColumnPosition(in.getColumnsNames().get(0).toString().toUpperCase());
								                              return t.getTupleList().get(pos).getValue();
								                              }
								                               });
								System.out.println("Secondary_"+tableName+"_"+in.getColumnsNames().get(0));
							}
						}
						/*if(pind.size()>1) // If we have more than 1 primary key (composite key)
						{
							System.out.println("Here conjoined primary key");
							if(!newTable.hasSecondaryIndexOn(pind.get(0))){
								secPrim1 = primaryIn.secondaryTreeMap("Secondary_"+tableName+"_"+pind.get(0), new SecondaryKeyExtractor<String, Long, MyTuple>() {
								public String extractSecondaryKey(Long key, MyTuple t)
								{
									int pos = t.getColumnPosition(pind.get(0).toUpperCase());
									return t.getTupleList().get(pos).getValue();
								}
								});
								System.out.println("Secondary_"+tableName+"_"+pind.get(0));
							}
							if(!newTable.hasSecondaryIndexOn(pind.get(1))){
								
								secPrim2 = primaryIn.secondaryTreeMap("Secondary_"+tableName+"_"+pind.get(1), new SecondaryKeyExtractor<String, Long, MyTuple>() {
								public String extractSecondaryKey(Long key, MyTuple t)
								{
									int pos = t.getColumnPosition(pind.get(1).toUpperCase());
									return t.getTupleList().get(pos).getValue();
								}
								});
								System.out.println("Secondary_"+tableName+"_"+pind.get(1));
							}
						}*/
						FileReader fr = new FileReader(tableDataFile);
						BufferedReader bf = new BufferedReader(fr);
						String tupleString;
						int commitMe = 0;
						while((tupleString=bf.readLine())!= null){
							MyTuple t = Utility.stringToTuple(tupleString, newTable);
							   String key = t.getTupleList().get(0).getValue();
							   Long longKey = Long.parseLong(key);
							   Index in = (Index) indices.get(0);
							   //I don't know how to handle composite keys with PrimaryStoreMap
							   if(in.getColumnsNames().size()>1)
							   {
							   	int pos = newTable.getColumnPosition(in.getColumnsNames().get(1).toString().toUpperCase());
							   	key+= t.getTupleList().get(pos).getValue();
							   	//System.out.println(key);
							   	longKey = Long.parseLong(key);
							   }
							
	                        primaryIn.putValue(t);
							commitMe++;
							if(commitMe == 5000) 
							{
								recman.commit();recman.clearCache();
								commitMe = 0;
							}
						}
						bf.close(); fr.close(); recman.close();
					} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					}
				
			}
		}
	}
	public IndexScanDetail getUpperAndLowerBounds(String where){
		IndexScanDetail retval =null;
		ArrayList<String> rangeBounds = new ArrayList<String>();
		String[] conditions = where.split(" AND | and ");
		String next = "";
		for(int i = 0; i < conditions.length; i++){
		next = conditions[i];
		String[] parts = next.split(" = | <> | > | < | >= | <= ");
		if(parts[0].contains(".") && parts[1].contains(".")){
			String leftTable = parts[0].substring(0, parts[0].indexOf(".")).toUpperCase().trim().
			replace("(", "").replace(")", "");;
			String rightTable = parts[1].substring(0, parts[1].indexOf(".")).toUpperCase().trim().
			replace("(", "").replace(")", "");;
			/* If the two tables are not equal then this is a join condition and
			* we can ignore it. */
			if(leftTable.equals(rightTable) || conditions[i].contains(" OR ")){
				/* this is an additional condition that we still have to evaluate */
				ArrayList<String> tableConds = new ArrayList<String>();
				tableConds.add(conditions[i]);
			}
		}else{
		/* if this is an inequality then there's nothing we can with it */
			if(!(conditions[i].contains("<>")) && !(conditions[i].contains(" = "))){
				rangeBounds.add(conditions[i]);
			}else{
				/* this is an additional condition that we still have to evaluate */
				String table = conditions[i].substring(
				0,conditions[i].indexOf(".")).trim().toUpperCase().
				replace("(", "").replace(")", "");
				ArrayList<String> tableConds = new ArrayList<String>();
				tableConds.add(conditions[i]);
			}
		}
		}
		/* Store the lower and upper bound for columns within range conditions */
		if(!(rangeBounds.isEmpty())){
				/* HACK assumes there's one pair of range conditions */
				String upperBound = "";
				String lowerBound = "";
				String column = "";
				String lowerBoundOperator = "";
				String upperBoundOperator = "";
				String tableName = "";
				String[] bounds = new String[2];
				String[] operators = new String[2];
				int i = 0;
				for(String cond: rangeBounds){
				String[] parts = cond.split(" = | <> | > | < | >= | <= ");
				column = cond.substring(cond.indexOf(".")+1, cond.indexOf(" ")).trim();
				operators[i] = cond.substring(cond.indexOf(" "), cond.lastIndexOf(" ")).trim();
				bounds[i] = parts[1].trim();
				i++;
			}
			if(bounds[0].compareTo(bounds[1]) > 0){
				upperBound = bounds[0];
				upperBoundOperator = operators[0];
				lowerBound = bounds[1];
				lowerBoundOperator = operators[1];
			}else{
				upperBound = bounds[1];
				upperBoundOperator = operators[1];
				lowerBound = bounds[0];
				lowerBoundOperator = operators[0];
			}
			String left = rangeBounds.get(0).replace(tableName.toLowerCase()+".", "");
			String right = rangeBounds.get(1).replace(tableName.toLowerCase()+".", "");
			if(upperBound.contains("DATE") || 
			upperBound.contains("date")){
			/* Parse out the date values */
					String dateUpper = upperBound;
					upperBound = dateUpper.substring(dateUpper.indexOf("(")+1,
					dateUpper.indexOf(")")).replace("'", "");
					String dateLower = lowerBound;
					lowerBound = dateLower.substring(dateLower.indexOf("(")+1,
					dateLower.indexOf(")")).replace("'", "");
			}
			IndexScanDetail isd = new IndexScanDetail(lowerBound, 
			upperBound, lowerBoundOperator, upperBoundOperator, column,
			left + " AND " + right, rangeBounds.get(0) + " AND " + rangeBounds.get(1));
			retval = isd;
		}
		return retval;
		}


}
